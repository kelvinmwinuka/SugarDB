// Copyright 2024 Kelvin Clement Mwinuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sugardb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/echovault/sugardb/internal"
	"github.com/echovault/sugardb/internal/aof"
	"github.com/echovault/sugardb/internal/clock"
	"github.com/echovault/sugardb/internal/config"
	"github.com/echovault/sugardb/internal/constants"
	"github.com/echovault/sugardb/internal/events"
	"github.com/echovault/sugardb/internal/eviction"
	"github.com/echovault/sugardb/internal/memberlist"
	"github.com/echovault/sugardb/internal/modules/acl"
	"github.com/echovault/sugardb/internal/modules/admin"
	"github.com/echovault/sugardb/internal/modules/connection"
	"github.com/echovault/sugardb/internal/modules/generic"
	"github.com/echovault/sugardb/internal/modules/hash"
	"github.com/echovault/sugardb/internal/modules/list"
	"github.com/echovault/sugardb/internal/modules/pubsub"
	"github.com/echovault/sugardb/internal/modules/set"
	"github.com/echovault/sugardb/internal/modules/sorted_set"
	str "github.com/echovault/sugardb/internal/modules/string"
	"github.com/echovault/sugardb/internal/raft"
	"github.com/echovault/sugardb/internal/snapshot"
	lua "github.com/yuin/gopher-lua"
	"io"
	"log"
	"net"
	"os"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type SugarDB struct {
	// clock is an implementation of a time interface that allows mocking of time functions during testing.
	clock clock.Clock

	// config holds the SugarDB configuration variables.
	config config.Config

	// The event queue which handles SugarDB events.
	eventQueue *events.EventQueue

	// The current index for the latest connection id.
	// This number is incremented everytime there's a new connection and
	// the new number is the new connection's ID.
	connId atomic.Uint64

	// connInfo holds the connection information for embedded and TCP clients.
	// It keeps track of the protocol and database that each client is operating on.
	connInfo struct {
		mut        *sync.RWMutex                         // RWMutex for the connInfo object.
		tcpClients map[*net.Conn]internal.ConnectionInfo // Map that holds connection information for each TCP client.
		embedded   internal.ConnectionInfo               // Information for the embedded connection.
	}

	// Global read-write mutex for entire store.
	storeLock *sync.RWMutex

	// Data store to hold the keys and their associated data, expiry time, etc.
	// The int key on the outer map represents the database index.
	// Each database has a map that has a string key and the key data (value and expiry time).
	store map[int]map[string]internal.KeyData

	// memUsed tracks the memory usage of the data in the store.
	memUsed int64

	// Holds all the keys that are currently associated with an expiry.
	keysWithExpiry struct {
		// Mutex as only one process should be able to update this list at a time.
		rwMutex sync.RWMutex
		// A map holding a string slice of the volatile keys for each database.
		keys map[int][]string
	}
	// LFU cache used when eviction policy is allkeys-lfu or volatile-lfu.
	lfuCache struct {
		// Mutex as only one goroutine can edit the LFU cache at a time.
		mutex *sync.Mutex
		// LFU cache for each database represented by a min heap.
		cache map[int]*eviction.CacheLFU
	}
	// LRU cache used when eviction policy is allkeys-lru or volatile-lru.
	lruCache struct {
		// Mutex as only one goroutine can edit the LRU at a time.
		mutex *sync.Mutex
		// LRU cache represented by a max heap.
		cache map[int]*eviction.CacheLRU
	}

	commandsRWMut sync.RWMutex       // Mutex used for modifying/reading the list of commands in the instance.
	commands      []internal.Command // Holds the list of all commands supported by SugarDB.

	// Each commands that's added using a script (lua,js), will have a lock associated with the command.
	// Only one goroutine will be able to trigger a script-associated command at a time. This is because the VM state
	// for each of the commands is not thread safe.
	// This map's shape is map[string]struct{vm: any, lock: sync.Mutex} with the string key being the command name.
	scriptVMs sync.Map

	raft       *raft.Raft             // The raft replication layer for SugarDB.
	memberList *memberlist.MemberList // The memberlist layer for SugarDB.

	context           context.Context
	contextCancelFunc context.CancelFunc

	acl    *acl.ACL
	pubSub *pubsub.PubSub

	latestSnapshotMilliseconds atomic.Int64     // Unix epoch in milliseconds.
	snapshotEngine             *snapshot.Engine // Snapshot engine for standalone mode.
	aofEngine                  *aof.Engine      // AOF engine for standalone mode.

	listener atomic.Value // Holds the TCP listener.
}

// NewSugarDB creates a new SugarDB instance.
// This functions accepts the WithContext, WithConfig and WithCommands options.
func NewSugarDB(options ...func(sugarDB *SugarDB)) (*SugarDB, error) {
	sugarDB := &SugarDB{
		clock:             clock.NewClock(),
		context:           context.Background(),
		contextCancelFunc: func() {},
		config:            config.DefaultConfig(),
		connInfo: struct {
			mut        *sync.RWMutex
			tcpClients map[*net.Conn]internal.ConnectionInfo
			embedded   internal.ConnectionInfo
		}{
			mut:        &sync.RWMutex{},
			tcpClients: make(map[*net.Conn]internal.ConnectionInfo),
			embedded: internal.ConnectionInfo{
				Id:       0,
				Name:     "embedded",
				Protocol: 2,
				Database: 0,
			},
		},
		storeLock: &sync.RWMutex{},
		store:     make(map[int]map[string]internal.KeyData),
		memUsed:   0,
		keysWithExpiry: struct {
			rwMutex sync.RWMutex
			keys    map[int][]string
		}{
			rwMutex: sync.RWMutex{},
			keys:    make(map[int][]string),
		},
		commandsRWMut: sync.RWMutex{},
		commands: func() []internal.Command {
			var commands []internal.Command
			commands = append(commands, acl.Commands()...)
			commands = append(commands, admin.Commands()...)
			commands = append(commands, connection.Commands()...)
			commands = append(commands, generic.Commands()...)
			commands = append(commands, hash.Commands()...)
			commands = append(commands, list.Commands()...)
			commands = append(commands, pubsub.Commands()...)
			commands = append(commands, set.Commands()...)
			commands = append(commands, sorted_set.Commands()...)
			commands = append(commands, str.Commands()...)
			return commands
		}(),
	}

	for _, option := range options {
		option(sugarDB)
	}

	sugarDB.context, sugarDB.contextCancelFunc = context.WithCancel(context.WithValue(
		sugarDB.context, "ServerID",
		internal.ContextServerID(sugarDB.config.ServerID),
	))

	// Setup event queue
	sugarDB.eventQueue = events.NewEventQueue(sugarDB.context)

	// Load .so modules from config
	for _, path := range sugarDB.config.Modules {
		if err := sugarDB.LoadModule(path); err != nil {
			log.Printf("%s %v\n", path, err)
			continue
		}
		log.Printf("loaded plugin %s\n", path)
	}

	// Set up ACL module
	sugarDB.acl = acl.NewACL(sugarDB.config)

	// Set up Pub/Sub module
	sugarDB.pubSub = pubsub.NewPubSub(sugarDB.context)

	if sugarDB.isInCluster() {
		sugarDB.raft = raft.NewRaft(
			sugarDB.context,
			raft.Opts{
				Store:                 &sugarDB.store,
				StoreLock:             sugarDB.storeLock,
				Config:                sugarDB.config,
				GetCommand:            sugarDB.getCommand,
				SetExpiry:             sugarDB.setExpiry,
				SetLatestSnapshotTime: sugarDB.setLatestSnapshot,
				GetHandlerFuncParams:  sugarDB.getHandlerFuncParams,
				SetValues:             sugarDB.setValues,
				DeleteKeys:            sugarDB.deleteKeys,
			})
		sugarDB.memberList = memberlist.NewMemberList(memberlist.Opts{
			Config:           sugarDB.config,
			HasJoinedCluster: sugarDB.raft.HasJoinedCluster,
			AddVoter:         sugarDB.raft.AddVoter,
			RemoveRaftServer: sugarDB.raft.RemoveServer,
			IsRaftLeader:     sugarDB.raft.IsRaftLeader,
			ApplyMutate:      sugarDB.raftApplyCommand,
			ApplyDeleteKeys:  sugarDB.raftApplyDeleteKeys,
		})
	} else {
		// Set up standalone snapshot engine
		sugarDB.snapshotEngine = snapshot.NewSnapshotEngine(
			sugarDB.context,
			snapshot.WithClock(sugarDB.clock),
			snapshot.WithDirectory(sugarDB.config.DataDir),
			snapshot.WithStore(sugarDB.store, sugarDB.storeLock),
			snapshot.WithInterval(sugarDB.config.SnapshotInterval),
			snapshot.WithThreshold(sugarDB.config.SnapShotThreshold),
			snapshot.WithSetLatestSnapshotTimeFunc(sugarDB.setLatestSnapshot),
			snapshot.WithGetLatestSnapshotTimeFunc(sugarDB.getLatestSnapshotTime),
			snapshot.WithEmitEventFunc(func(e events.Event) {
				sugarDB.eventQueue.Enqueue(e)
			}),
			snapshot.WithSetKeyDataFunc(func(database int, key string, data internal.KeyData) {
				ctx := context.WithValue(context.Background(), "Database", database)
				if err := sugarDB.setValues(ctx, map[string]interface{}{key: data.Value}); err != nil {
					log.Println(err)
				}
				sugarDB.setExpiry(ctx, key, data.ExpireAt, false)
			}),
		)

		// Set up standalone AOF engine
		aofEngine, err := aof.NewAOFEngine(
			sugarDB.context,
			aof.WithStore(sugarDB.store, sugarDB.storeLock),
			aof.WithClock(sugarDB.clock),
			aof.WithDirectory(sugarDB.config.DataDir),
			aof.WithStrategy(sugarDB.config.AOFSyncStrategy),
			aof.WithSetKeyDataFunc(func(database int, key string, value internal.KeyData) {
				ctx := context.WithValue(context.Background(), "Database", database)
				if err := sugarDB.setValues(ctx, map[string]interface{}{key: value.Value}); err != nil {
					log.Println(err)
				}
				sugarDB.setExpiry(ctx, key, value.ExpireAt, false)
			}),
			aof.WithHandleCommandFunc(func(database int, command []byte) {
				ctx := context.WithValue(context.Background(), "Protocol", 2)
				ctx = context.WithValue(ctx, "Database", database)
				_, err := sugarDB.handleCommand(ctx, command, nil, true, false)
				if err != nil {
					log.Println(err)
				}
			}),
		)
		if err != nil {
			return nil, err
		}
		sugarDB.aofEngine = aofEngine
	}

	// If eviction policy is not noeviction, start a goroutine to evict keys at the configured interval.

	if sugarDB.config.EvictionPolicy != constants.NoEviction {
		go func() {
			ticker := time.NewTicker(sugarDB.config.EvictionInterval)
			defer ticker.Stop()

			currentDB := 0 // The current database to evict expired keys from

			for {
				select {
				case <-sugarDB.context.Done():
					log.Println("shutting down ttl eviction routine...")
					return
				case <-ticker.C:
					// Emit event for key eviction in databases that have volatile keys.
					sugarDB.eventQueue.Enqueue(events.Event{
						Kind:     events.EVENT_KIND_TTL_EVICTION,
						Priority: events.EVENT_PRIORITY_HIGH,
						Time:     sugarDB.clock.Now(),
						Handler: func() error {
							sugarDB.storeLock.Lock()
							defer sugarDB.storeLock.Unlock()

							// Otherwise, carry out the eviction.
							ctx := context.WithValue(context.Background(), "Database", currentDB)
							err := sugarDB.evictKeysWithExpiredTTL(ctx)

							sugarDB.keysWithExpiry.rwMutex.RLock()
							defer sugarDB.keysWithExpiry.rwMutex.RUnlock()

							// Get the list of dbs and sort in ascending order.
							var dbs []int
							for db := range sugarDB.keysWithExpiry.keys {
								dbs = append(dbs, db)
							}
							sort.Slice(dbs, func(i, j int) bool {
								return dbs[i] < dbs[j]
							})

							// Pick the next database.
							idx := slices.Index(dbs, currentDB)
							if idx < len(dbs)-1 {
								currentDB = dbs[idx+1]
							} else if idx >= len(dbs)-1 {
								// If the current db is the last one, set database 0 as the current one.
								currentDB = 0
							}

							return err
						},
					})
				}
			}
		}()
	}

	if sugarDB.config.TLS && len(sugarDB.config.CertKeyPairs) <= 0 {
		return nil, errors.New("must provide certificate and key file paths for TLS mode")
	}

	// Initialise caches
	sugarDB.initialiseCaches()

	if sugarDB.isInCluster() {
		// Initialise raft and memberlist
		sugarDB.raft.RaftInit()
		sugarDB.memberList.MemberListInit(sugarDB.context)
	}

	if !sugarDB.isInCluster() {
		if sugarDB.config.RestoreAOF {
			err := sugarDB.aofEngine.Restore()
			if err != nil {
				log.Println(err)
			}
		}

		// Restore from snapshot if snapshot restore is enabled and AOF restore is disabled
		if sugarDB.config.RestoreSnapshot && !sugarDB.config.RestoreAOF {
			err := sugarDB.snapshotEngine.Restore()
			if err != nil {
				log.Println(err)
			}
		}
	}

	return sugarDB, nil
}

func (server *SugarDB) startTCP() {
	conf := server.config

	listenConfig := net.ListenConfig{
		KeepAlive: 200 * time.Millisecond,
	}

	listener, err := listenConfig.Listen(
		server.context,
		"tcp",
		fmt.Sprintf("%s:%d", conf.BindAddr, conf.Port),
	)
	if err != nil {
		log.Printf("listener error: %v", err)
		return
	}

	if !conf.TLS {
		// TCP
		log.Printf("Starting TCP server at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
	}

	if conf.TLS || conf.MTLS {
		// TLS
		if conf.MTLS {
			log.Printf("Starting mTLS server at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
		} else {
			log.Printf("Starting TLS server at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
		}

		var certificates []tls.Certificate
		for _, certKeyPair := range conf.CertKeyPairs {
			c, err := tls.LoadX509KeyPair(certKeyPair[0], certKeyPair[1])
			if err != nil {
				log.Printf("load cert key pair: %v\n", err)
				return
			}
			certificates = append(certificates, c)
		}

		clientAuth := tls.NoClientCert
		clientCerts := x509.NewCertPool()

		if conf.MTLS {
			clientAuth = tls.RequireAndVerifyClientCert
			for _, c := range conf.ClientCAs {
				ca, err := os.Open(c)
				if err != nil {
					log.Printf("client cert open: %v\n", err)
					return
				}
				certBytes, err := io.ReadAll(ca)
				if err != nil {
					log.Printf("client cert read: %v\n", err)
				}
				if ok := clientCerts.AppendCertsFromPEM(certBytes); !ok {
					log.Printf("client cert append: %v\n", err)
				}
			}
		}

		listener = tls.NewListener(listener, &tls.Config{
			Certificates: certificates,
			ClientAuth:   clientAuth,
			ClientCAs:    clientCerts,
		})
	}

	server.listener.Store(listener)

	// Listen to connection.
	for {
		select {
		case <-server.context.Done():
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("listener error: %v\n", err)
				return
			}
			// Read loop for connection
			go server.handleConnection(conn)
		}
	}
}

func (server *SugarDB) handleConnection(conn net.Conn) {
	// If ACL module is loaded, register the connection with the ACL
	if server.acl != nil {
		server.acl.RegisterConnection(&conn)
	}

	r := io.Reader(conn)

	// Generate connection ID
	cid := server.connId.Add(1)
	ctx := context.WithValue(server.context, internal.ContextConnID("ConnectionID"),
		fmt.Sprintf("%s-%d", server.context.Value(internal.ContextServerID("ServerID")), cid))

	// Set the default connection information
	server.connInfo.mut.Lock()
	server.connInfo.tcpClients[&conn] = internal.ConnectionInfo{
		Id:       cid,
		Name:     "",
		Protocol: 2,
		Database: 0,
	}
	server.connInfo.mut.Unlock()

	defer func() {
		log.Printf("closing connection %d...", cid)
		if err := conn.Close(); err != nil {
			log.Println(err)
		}
	}()

	for {
		select {
		case <-server.context.Done():
			break
		default:
			message, err := internal.ReadMessage(r)

			if err != nil && errors.Is(err, io.EOF) {
				// Connection closed
				log.Println(err)
				break
			}

			if err != nil {
				log.Println(err)
				break
			}

			// If the message is empty, break the loop
			// an empty message will be read when the client runs the "quit" command.
			if len(message) == 0 {
				break
			}

			// Add this command to the event queue
			server.eventQueue.Enqueue(events.Event{
				Kind:     events.EVENT_KIND_COMMAND,
				Priority: events.EVENT_PRIORITY_MEDIUM,
				Time:     server.clock.Now(),
				Handler: func() error {
					w := io.Writer(conn)
					res, err := server.handleCommand(ctx, message, &conn, false, false)
					if err != nil {
						_, _ = w.Write([]byte(fmt.Sprintf("-Error %s\r\n", err.Error())))
					}

					chunkSize := 1024

					// If the length of the response is 0, return nothing to the client.
					if len(res) == 0 {
						return nil
					}

					if len(res) <= chunkSize {
						_, _ = w.Write(res)
						return nil
					}

					// If the response is large, send it in chunks.
					startIndex := 0
					for {
						// If the current start index is less than chunkSize from length, return the remaining bytes.
						if len(res)-1-startIndex < chunkSize {
							_, err = w.Write(res[startIndex:])
							if err != nil {
								log.Println(err)
							}
							break
						}
						n, _ := w.Write(res[startIndex : startIndex+chunkSize])
						if n < chunkSize {
							break
						}
						startIndex += chunkSize
					}
					return nil
				},
			})
		}
	}
}

// Start starts the SugarDB instance's TCP listener.
// This allows the instance to accept connections handle client commands over TCP.
//
// You can still use command functions like Set if you're embedding SugarDB in your application.
// However, if you'd like to also accept TCP request on the same instance, you must call this function.
func (server *SugarDB) Start() {
	server.startTCP()
}

// takeSnapshot emits an event to take a snapshot of the store when called.
func (server *SugarDB) takeSnapshot() {
	server.eventQueue.Enqueue(events.Event{
		Kind:     events.EVENT_KIND_SNAPSHOT,
		Priority: events.EVENT_PRIORITY_HIGH,
		Time:     server.clock.Now(),
		Handler: func() error {
			if server.isInCluster() {
				// Handle snapshot in cluster mode
				return server.raft.TakeSnapshot()
			}
			return server.snapshotEngine.TakeSnapshot()
		},
	})
}

func (server *SugarDB) setLatestSnapshot(msec int64) {
	server.latestSnapshotMilliseconds.Store(msec)
}

// getLatestSnapshotTime returns the latest snapshot time in unix epoch milliseconds.
func (server *SugarDB) getLatestSnapshotTime() int64 {
	return server.latestSnapshotMilliseconds.Load()
}

// ShutDown gracefully shuts down the SugarDB instance.
// This function shuts down the memberlist and raft layers.
func (server *SugarDB) ShutDown() {
	if server.listener.Load() != nil {
		log.Println("closing tcp listener...")
		if err := server.listener.Load().(net.Listener).Close(); err != nil {
			log.Printf("listener close: %v\n", err)
		}
	}

	// Shutdown all script VMs
	log.Println("shutting down script vms...")
	server.commandsRWMut.Lock()
	for _, command := range server.commands {
		if slices.Contains([]string{"LUA_SCRIPT", "JS_SCRIPT"}, command.Type) {
			v, ok := server.scriptVMs.Load(command.Command)
			if !ok {
				continue
			}
			machine := v.(struct {
				vm   any
				lock *sync.Mutex
			})
			machine.lock.Lock()
			switch command.Type {
			case "LUA_SCRIPT":
				machine.vm.(*lua.LState).Close()
			}
			machine.lock.Unlock()
		}
	}
	server.commandsRWMut.Unlock()

	if server.isInCluster() {
		// Server is in cluster, run cluster-only shutdown processes.
		server.raft.RaftShutdown()
		server.memberList.MemberListShutdown()
	}

	server.contextCancelFunc()
}

func (server *SugarDB) initialiseCaches() {
	// Set up LFU cache.
	server.lfuCache = struct {
		mutex *sync.Mutex
		cache map[int]*eviction.CacheLFU
	}{
		mutex: &sync.Mutex{},
		cache: make(map[int]*eviction.CacheLFU),
	}
	// set up LRU cache.
	server.lruCache = struct {
		mutex *sync.Mutex
		cache map[int]*eviction.CacheLRU
	}{
		mutex: &sync.Mutex{},
		cache: make(map[int]*eviction.CacheLRU),
	}
	// Initialise caches for each preloaded database.
	for database, _ := range server.store {
		server.lfuCache.cache[database] = eviction.NewCacheLFU()
		server.lruCache.cache[database] = eviction.NewCacheLRU()
	}
}
