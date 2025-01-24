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
	mux       sync.Mutex
}

func (queue *EventQueue) Enqueue(e Event) {
	queue.mux.Lock()
	defer queue.mux.Unlock()
	heap.Push(&queue.eventHeap, e)
	// TODO: Uncomment this
	// log.Printf("event queued: %#+v\n", e)
}

func (queue *EventQueue) dequeue() any {
	queue.mux.Lock()
	defer queue.mux.Unlock()
	if queue.len() == 0 {
		return nil
	}
	e := heap.Pop(&queue.eventHeap).(Event)
	return e
}

func (queue *EventQueue) len() int {
	return (&queue.eventHeap).Len()
}

func NewEventQueue(ctx context.Context) *EventQueue {
	queue := &EventQueue{
		eventHeap: make(eventHeap, 0),
		mux:       sync.Mutex{},
	}

	heap.Init(&queue.eventHeap)

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("closing event loop...")
				return
			default:
				event := queue.dequeue()
				if event == nil {
					continue
				}
				e := event.(Event)

				// log.Printf("processing event: %#+v\n", e)

				// Invoke event handler
				if err := e.Handler(); err != nil {
					log.Printf("error %+v on %s event: %+v", e.Kind, err, e)
				}
			}
		}
	}()
	return queue
}
