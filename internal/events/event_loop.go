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

package events

import (
	"container/heap"
	"context"
	"log"
	"sync"
)

type eventHeap []Event

// Len implements the sort interface
func (h *eventHeap) Len() int {
	return len(*h)
}

// Less implements the sort interface
func (h *eventHeap) Less(i, j int) bool {
	// If priorities are the same, earlier event should be processed first
	if (*h)[i].Priority == (*h)[j].Priority {
		return (*h)[i].Time.Before((*h)[j].Time)
	}
	// Otherwise, higher priority events should be processed first
	return (*h)[i].Priority > (*h)[j].Priority
}

// Swap implements the sort interface
func (h *eventHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

// Pop implements the heap interface
func (h *eventHeap) Pop() any {
	old := *h
	e := old[len(old)-1]     // Get the last element of the heap
	*h = old[0 : len(old)-1] // Remove the last element from the original slice
	return e
}

func (h *eventHeap) Push(e any) {
	*h = append(*h, e.(Event))
}

type EventQueue struct {
	eventHeap eventHeap
	mux       *sync.Mutex
	cond      *sync.Cond
}

func (queue *EventQueue) Enqueue(e Event) {
	queue.mux.Lock()
	defer queue.mux.Unlock()
	heap.Push(&queue.eventHeap, e)
	queue.cond.Signal()
}

func (queue *EventQueue) dequeue(batchSize int) []Event {
	queue.mux.Lock()
	defer queue.mux.Unlock()

	for queue.len() < batchSize {
		queue.cond.Wait()
	}

	var events []Event
	for i := 0; i < batchSize && queue.len() > 0; i++ {
		e := heap.Pop(&queue.eventHeap).(Event)
		events = append(events, e)
	}

	return events
}

func (queue *EventQueue) len() int {
	return (&queue.eventHeap).Len()
}

func NewEventQueue(ctx context.Context) *EventQueue {
	queue := &EventQueue{
		eventHeap: make(eventHeap, 0),
		mux:       &sync.Mutex{},
	}
	queue.cond = sync.NewCond(queue.mux)

	heap.Init(&queue.eventHeap)

	numOfWorkers := 10

	for i := 0; i < numOfWorkers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					log.Println("closing event loop...")
					return
				default:
					events := queue.dequeue(1)
					for _, event := range events {
						go func(e Event) {
							// Invoke event handler
							if err := e.Handler(); err != nil {
								log.Printf("error %+v on %s event: %+v", e.Kind, err, e)
							}
						}(event)
					}
				}
			}
		}()
	}

	return queue
}
