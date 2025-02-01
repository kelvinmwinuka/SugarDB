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

package store

import (
	"github.com/echovault/sugardb/internal"
	"sync/atomic"
)

type Store struct {
	buckets []*atomic.Pointer[lockFreeInnerMap] // Slice containing the databases/keyspaces
	size    int                                 // Number of databases/keyspaces in the store
}

func NewStore(size int) *Store {
	buckets := make([]*atomic.Pointer[lockFreeInnerMap], size)
	for i, _ := range buckets {
		buckets[i] = &atomic.Pointer[lockFreeInnerMap]{}
	}
	return &Store{buckets: buckets, size: size}
}

func (s *Store) Set(database int, key string, value internal.KeyData) {
	for {
		innerMap := s.buckets[database].Load()

		// If no database exists, create a new one
		if innerMap == nil {
			newInnerMap := newLockFreeInnerMap(4096) // TODO: make this configurable
			if s.buckets[database].CompareAndSwap(nil, newInnerMap) {
				innerMap = newInnerMap
			} else {
				continue // Retry if another goroutine initialized it
			}
		}

		// Store the value in the inner map
		innerMap.Store(key, value)
		return
	}
}

func (s *Store) Get(database int, key string) (internal.KeyData, bool) {
	innerMap := s.buckets[database].Load()
	if innerMap == nil {
		return internal.KeyData{}, false
	}
	return innerMap.Lookup(key)
}

func (s *Store) Del(database int, key string) bool {
	innerMap := s.buckets[database].Load()
	if innerMap == nil {
		return false
	}
	return innerMap.Delete(key)
}

func (s *Store) Flush(database int) {
	innerMap := s.buckets[database].Load()
	innerMap.flush()
}

func (s *Store) DBSize(database int) int {
	innerMap := s.buckets[database].Load()
	return innerMap.dbSize()
}

func (s *Store) RandomKey(database int) string {
	innerMap := s.buckets[database].Load()
	return innerMap.randomKey()
}
