package sugardb

import (
	"container/heap"
	"fmt"
	"log"
	"sync"
)

type eventHeap []event

// Len implements the sort interface
func (h *eventHeap) Len() int {
	return len(*h)
}

// Less implements the sort interface
func (h *eventHeap) Less(i, j int) bool {
	// If priorities are the same, earlier event should be processed first
	if (*h)[i].priority == (*h)[j].priority {
		return (*h)[i].time.Before((*h)[j].time)
	}
	// Otherwise, higher priority events should be processed first
	return (*h)[i].priority > (*h)[j].priority
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
	*h = append(*h, e.(event))
}

type eventQueue struct {
	eventHeap eventHeap
	mux       sync.Mutex
}

func (queue *eventQueue) enqueue(e event) {
	queue.mux.Lock()
	defer queue.mux.Unlock()
	heap.Push(&queue.eventHeap, e)
	log.Printf("event queued: %#+v\n", e)
}

func (queue *eventQueue) dequeue() event {
	queue.mux.Lock()
	defer queue.mux.Unlock()
	e := heap.Pop(&queue.eventHeap).(event)
	return e
}

func (queue *eventQueue) len() int {
	queue.mux.Lock()
	defer queue.mux.Unlock()
	return (&queue.eventHeap).Len()
}

func newEventQueue() *eventQueue {
	queue := &eventQueue{
		eventHeap: make(eventHeap, 0),
		mux:       sync.Mutex{},
	}
	heap.Init(&queue.eventHeap)
	go func() {
		for {
			if queue.len() == 0 {
				continue
			}
			e := queue.dequeue()
			fmt.Printf("processing event: %#+v\n", e)
			// TODO: Switch statement to handle different events
		}
	}()
	return queue
}
