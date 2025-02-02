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
	"math/rand"
	"strings"
	"sync/atomic"
	"unsafe"
)

const maxLevels = 16 // TODO: Make this configurable

const probability = 0.5

type node struct {
	key   string
	value unsafe.Pointer
	next  [maxLevels]*atomic.Pointer[node]
}

type DBSkipList struct {
	head  *node
	level int64
}

func NewDBSkipList() *DBSkipList {
	head := &node{
		next: [maxLevels]*atomic.Pointer[node]{},
	}
	for i := 0; i < maxLevels; i++ {
		head.next[i] = &atomic.Pointer[node]{}
	}
	return &DBSkipList{
		head:  head,
		level: 1,
	}
}

// randomLevel generates a random height for a new node
func randomLevel() int {
	level := 0
	for rand.Float32() < probability && level < maxLevels {
		level++
	}
	return level
}

func (sl *DBSkipList) Find(key string) (internal.KeyData, bool) {
	curr := sl.head

	for i := atomic.LoadInt64(&sl.level); i >= 0; i-- {
		for next := curr.next[i].Load(); next != nil && strings.Compare(next.key, key) < 0; next = curr.next[i].Load() {
			curr = next
		}
	}

	for next := curr.next[0].Load(); next != nil; next = curr.next[0].Load() {
		if next.key == key {
			return *(*internal.KeyData)(next.value), true
		}
		curr = next
	}

	return internal.KeyData{}, false
}

func (sl *DBSkipList) Insert(key string, value internal.KeyData) {
	update := [maxLevels]*node{}
	curr := sl.head

	for i := atomic.LoadInt64(&sl.level); i >= 0; i-- {
		for next := curr.next[i].Load(); next != nil && strings.Compare(next.key, key) < 0; next = curr.next[i].Load() {
			curr = next
		}
		update[i] = curr
	}

	// If this node's level is greater than the current level of the skip list,
	// increase the skip list's level.
	level := randomLevel()
	if level > int(atomic.LoadInt64(&sl.level)) {
		atomic.StoreInt64(&sl.level, int64(level))
	}

	newNode := &node{
		key:   key,
		value: unsafe.Pointer(&value),
		next:  [maxLevels]*atomic.Pointer[node]{},
	}
	for i := 0; i < level; i++ {
		newNode.next[i] = &atomic.Pointer[node]{}
		for {
			next := update[i].next[i].Load()
			newNode.next[i].Store(next)
			if update[i].next[i].CompareAndSwap(next, newNode) {
				break
			}
		}
	}
}

func (sl *DBSkipList) Delete(key string) bool {
	update := [maxLevels]*node{}
	curr := sl.head
	var target *node

	for i := atomic.LoadInt64(&sl.level); i >= 0; i-- {
		for next := curr.next[i].Load(); next != nil && strings.Compare(next.key, key) < 0; next = curr.next[i].Load() {
			curr = next
		}
		update[i] = curr
	}

	for next := curr.next[0].Load(); next != nil; next = curr.next[0].Load() {
		if next.key == key {
			target = next
			break
		}
		curr = next
	}

	if target == nil {
		return false
	}

	for i := 0; i < int(atomic.LoadInt64(&sl.level)); i++ {
		for {
			next := target.next[i].Load()
			if update[i].next[i].CompareAndSwap(target, next) {
				break
			}
		}
	}

	return true
}
