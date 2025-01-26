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

package preamble

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/echovault/sugardb/internal"
	"github.com/echovault/sugardb/internal/clock"
	"io"
	"log"
	"os"
	"path"
	"sync"
)

type ReadWriter interface {
	io.ReadWriteSeeker
	io.Closer
	Truncate(size int64) error
	Sync() error
}

type Store struct {
	store          map[int]map[string]internal.KeyData
	clock          clock.Clock
	rw             ReadWriter
	mut            sync.Mutex
	directory      string
	setKeyDataFunc func(database int, key string, data internal.KeyData)
}

func WithStore(state map[int]map[string]internal.KeyData) func(store *Store) {
	return func(store *Store) {
		store.store = state
	}
}

func WithClock(clock clock.Clock) func(store *Store) {
	return func(store *Store) {
		store.clock = clock
	}
}

func WithReadWriter(rw ReadWriter) func(store *Store) {
	return func(store *Store) {
		store.rw = rw
	}
}

func WithSetKeyDataFunc(f func(database int, key string, data internal.KeyData)) func(store *Store) {
	return func(store *Store) {
		store.setKeyDataFunc = f
	}
}

func WithDirectory(directory string) func(store *Store) {
	return func(store *Store) {
		store.directory = directory
	}
}

func NewPreambleStore(ctx context.Context, options ...func(store *Store)) (*Store, error) {
	store := &Store{
		clock:          clock.NewClock(),
		rw:             nil,
		mut:            sync.Mutex{},
		directory:      "",
		setKeyDataFunc: func(database int, key string, data internal.KeyData) {},
	}

	for _, option := range options {
		option(store)
	}

	// If rw is nil, create the default
	if store.rw == nil && store.directory != "" {
		err := os.MkdirAll(path.Join(store.directory, "aof"), os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("new preamble store -> mkdir error: %+v", err)
		}
		f, err := os.OpenFile(path.Join(store.directory, "aof", "preamble.bin"), os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("new preamble store -> open file error: %+v", err)
		}
		store.rw = f
	}

	go func() {
		<-ctx.Done()
		log.Println("closing preamble store...")
		if err := store.close(); err != nil {
			log.Printf("close preamble store error: %+v\n", err)
		}
	}()

	return store, nil
}

func (store *Store) CreatePreamble() error {
	store.mut.Lock()
	store.mut.Unlock()

	// Get current state.
	state := internal.FilterExpiredKeys(store.clock.Now(), store.store)
	o, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// Truncate the preamble first
	if err = store.rw.Truncate(0); err != nil {
		return err
	}
	// Seek to the beginning of the file after truncating
	if _, err = store.rw.Seek(0, 0); err != nil {
		return err
	}

	if _, err = store.rw.Write(o); err != nil {
		return err
	}

	// Sync the changes
	if err = store.rw.Sync(); err != nil {
		return err
	}

	return nil
}

func (store *Store) Restore() error {
	if store.rw == nil {
		return nil
	}

	// Seek to the beginning of the file before beginning restore.
	if _, err := store.rw.Seek(0, 0); err != nil {
		return fmt.Errorf("restore preamble: %v", err)
	}

	b, err := io.ReadAll(store.rw)
	if err != nil {
		return err
	}

	if len(b) <= 0 {
		return nil
	}

	state := make(map[int]map[string]internal.KeyData)
	if err = json.Unmarshal(b, &state); err != nil {
		return err
	}

	for database, data := range internal.FilterExpiredKeys(store.clock.Now(), state) {
		for key, keyData := range data {
			store.setKeyDataFunc(database, key, keyData)
		}
	}

	return nil
}

func (store *Store) close() error {
	store.mut.Lock()
	defer store.mut.Unlock()
	if store.rw == nil {
		return nil
	}
	if err := store.rw.Close(); err != nil {
		return err
	}
	return nil
}
