package events

import (
	"container/heap"
	"context"
	"log"
	"net"
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
	eventHeap          eventHeap
	mux                sync.Mutex
	commandHandlerFunc func(
		ctx context.Context, message []byte, conn *net.Conn, replay bool, embedded bool,
	) ([]byte, error)
}

func (queue *EventQueue) Enqueue(e Event) {
	queue.mux.Lock()
	defer queue.mux.Unlock()
	heap.Push(&queue.eventHeap, e)
	// TODO: Uncomment this
	// log.Printf("event queued: %#+v\n", e)
}

func (queue *EventQueue) dequeue() Event {
	queue.mux.Lock()
	defer queue.mux.Unlock()
	e := heap.Pop(&queue.eventHeap).(Event)
	return e
}

func (queue *EventQueue) len() int {
	queue.mux.Lock()
	defer queue.mux.Unlock()
	return (&queue.eventHeap).Len()
}

func WithCommandHandlerFunc(commandHandlerFunc func(
	ctx context.Context, message []byte, conn *net.Conn, replay bool, embedded bool,
) ([]byte, error)) func(queue *EventQueue) {
	return func(queue *EventQueue) {
		queue.commandHandlerFunc = commandHandlerFunc
	}
}

func NewEventQueue(options ...func(queue *EventQueue)) *EventQueue {
	queue := &EventQueue{
		eventHeap: make(eventHeap, 0),
		mux:       sync.Mutex{},
	}

	heap.Init(&queue.eventHeap)

	for _, option := range options {
		option(queue)
	}

	go func() {
		for {
			if queue.len() == 0 {
				continue
			}
			e := queue.dequeue()
			log.Printf("processing event: %#+v\n", e)
			switch e.Kind {

			default:
				log.Printf("could not process event: %#+v\n", e)

			case EVENT_KIND_COMMAND:
				// Handle command event
				if err := e.Handler(); err != nil {
					log.Printf("error %+v on command event: %+v", err, e)
				}

			case EVENT_KIND_DELETE_KEY:
				// Handle delete key event
				if err := e.Handler(); err != nil {
					log.Printf("error %+v on delete keys event: %+v", err, e)
				}

			case EVENT_KIND_UPDATE_KEYS_IN_CACHE:
				// Handle events to update key status in caches (lfu, lru)
				if err := e.Handler(); err != nil {
					log.Printf("error %+v on update keys in cache event: %+v", err, e)
				}

			case EVENT_KIND_SNAPSHOT:
				// Handle snapshot event
				if err := e.Handler(); err != nil {
					log.Printf("error %+v on snapshot event: %+v", err, e)
				}

			case EVENT_KIND_UPDATE_CONFIG:
				// Handle config update event
				if err := e.Handler(); err != nil {
					log.Printf("error %+v on update config event: %+v", err, e)
				}
			}
		}
	}()
	return queue
}
