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
	"unsafe"
)

type dataNode struct {
	key   string
	value internal.KeyData
	next  *dataNode
}

type lockFreeInnerMap struct {
	buckets []*atomic.Pointer[dataNode] // Slice containing the linked lists (buckets)
	size    int                         // Number of buckets
}

func newLockFreeInnerMap(size int) *lockFreeInnerMap {
	buckets := make([]*atomic.Pointer[dataNode], size)
	for i, _ := range buckets {
		buckets[i] = &atomic.Pointer[dataNode]{}
	}
	return &lockFreeInnerMap{buckets: buckets, size: size}
}

// Hash function for the database/keyspace
func (m *lockFreeInnerMap) hash(key string) int {
	hash := 0
	for i := 0; i < len(key); i++ {
		hash = (hash*31 + int(key[i])) % m.size
	}
	return hash
}

// Store the value in the database/keyspace
func (m *lockFreeInnerMap) Store(key string, value internal.KeyData) {
	index := m.hash(key)
	newData := &dataNode{key: key, value: value}
	for {
		head := m.buckets[index].Load()
		newData.next = head
		// Attempt CAS
		if m.buckets[index].CompareAndSwap(head, newData) {
			return
		}
	}
}

func (m *lockFreeInnerMap) Lookup(key string) (internal.KeyData, bool) {
	index := m.hash(key)
	head := m.buckets[index].Load()
	for node := head; node != nil; node = node.next {
		if node.key == key {
			return node.value, true
		}
	}
	return internal.KeyData{}, false
}

func (m *lockFreeInnerMap) Delete(key string) bool {
	index := m.hash(key)

	for {
		head := m.buckets[index].Load()
		if head == nil {
			return false
		}

		// If the head node matches the key, try to delete it with CAS
		if head.key == key {
			newHead := head.next
			if m.buckets[index].CompareAndSwap(head, newHead) {
				return true
			}
			continue // Retry if CAS fails
		}

		// Traverse the list to find and remove the node
		prev := head
		for node := head.next; node != nil; node = node.next {
			if node.key == key {
				newNext := node.next
				if atomic.CompareAndSwapPointer(
					(*unsafe.Pointer)(unsafe.Pointer(&prev.next)),
					unsafe.Pointer(node),
					unsafe.Pointer(newNext),
				) {
					return true // Successfully deleted
				}
				break // Restart if CAS fails
			}
			prev = node
		}

		return false // Key not found
	}
}
