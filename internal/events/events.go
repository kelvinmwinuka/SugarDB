package events

import (
	"time"
)

type EventPriority int

var (
	EVENT_PRIORITY_HIGH   = EventPriority(2)
	EVENT_PRIORITY_MEDIUM = EventPriority(1)
)

type EventKind string

var (
	EVENT_KIND_COMMAND              = EventKind("command")
	EVENT_KIND_SNAPSHOT             = EventKind("snapshot")
	EVENT_KIND_DELETE_KEY           = EventKind("delete-key")
	EVENT_KIND_UPDATE_KEYS_IN_CACHE = EventKind("update-keys-in-cache")
	EVENT_KIND_UPDATE_CONFIG        = EventKind("update-config")
)

type Event struct {
	Kind     EventKind     // The type of event
	Priority EventPriority // The event priority, higher priority events will be handled first
	Handler  func() error  // The event handler

	// Time is the time the event was generated, earlier events are processed first if they have the same priority
	Time time.Time
}
