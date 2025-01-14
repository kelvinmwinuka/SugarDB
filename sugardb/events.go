package sugardb

import (
	"time"
)

type eventPriority int

var (
	event_priority_high   = eventPriority(2)
	event_priority_medium = eventPriority(1)
)

type eventKind string

var (
	event_kind_command    = eventKind("command")
	event_kind_delete_key = eventKind("delete-key")
)

type event struct {
	kind     eventKind
	priority eventPriority
	time     time.Time
}
