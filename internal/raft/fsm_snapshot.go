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

package raft

import (
	"encoding/json"
	"github.com/echovault/sugardb/internal"
	"github.com/echovault/sugardb/internal/config"
	"github.com/hashicorp/raft"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SnapshotOpts struct {
	config                config.Config
	store                 *map[int]map[string]internal.KeyData
	storeLock             *sync.RWMutex
	setLatestSnapshotTime func(msec int64)
}

type Snapshot struct {
	options SnapshotOpts
}

func NewFSMSnapshot(opts SnapshotOpts) *Snapshot {
	return &Snapshot{
		options: opts,
	}
}

// Persist implements FSMSnapshot interface
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	msec, err := strconv.Atoi(strings.Split(sink.ID(), "-")[2])
	if err != nil {
		_ = sink.Cancel()
		return err
	}

	s.options.storeLock.RLock()
	snapshotObject := internal.SnapshotObject{
		State:                      internal.FilterExpiredKeys(time.Now(), *s.options.store),
		LatestSnapshotMilliseconds: int64(msec),
	}

	o, err := json.Marshal(snapshotObject)

	if err != nil {
		_ = sink.Cancel()
		return err
	}

	if _, err = sink.Write(o); err != nil {
		_ = sink.Cancel()
		return err
	}

	s.options.setLatestSnapshotTime(int64(msec))

	return nil
}

// Release implements FSMSnapshot interface
func (s *Snapshot) Release() {
	defer s.options.storeLock.RUnlock()
}
