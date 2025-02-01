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
	"github.com/cespare/xxhash/v2"
	"github.com/echovault/sugardb/internal"
	"math/rand"
	"sync"
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
	hash := xxhash.Sum64([]byte(key)) // Generate 64-bit hash
	return int(hash % uint64(m.size)) // Map to bucket range
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

func (m *lockFreeInnerMap) flush() {
	newBuckets := make([]*atomic.Pointer[dataNode], m.size)
	for i, _ := range newBuckets {
		newBuckets[i] = &atomic.Pointer[dataNode]{}
	}
	atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&m.buckets)), unsafe.Pointer(&newBuckets))
}

func (m *lockFreeInnerMap) dbSize() int {
	var total atomic.Int64

	wg := sync.WaitGroup{}
	for i := 0; i < m.size; i++ {
		wg.Add(1)
		go func(bucketIdx int) {
			node := m.buckets[bucketIdx].Load()
			for node != nil {
				mem, _ := node.value.GetMem()
				total.Add(mem)
				node = node.next
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	return int(total.Load())
}

func (m *lockFreeInnerMap) randomKey() string {
	// Pick a random bucket
	for {
		index := rand.Intn(m.size)

		head := m.buckets[index].Load()
		if head == nil {
			continue // Retry if head in nil
		}

		// Count the nodes in the linked list
		var count int
		for node := head; node != nil; node = node.next {
			count++
		}

		// Pick a random node
		randomIndex := rand.Intn(count)
		current := head
		for i := 0; i < randomIndex; i++ {
			current = current.next
		}

		return current.key
	}
}
