package events

import (
	"context"
	"net"
	"time"
)

type EventPriority int

var (
	EVENT_PRIORITY_HIGH   = EventPriority(2)
	EVENT_PRIORITY_MEDIUM = EventPriority(1)
)

type EventKind string

var (
	EVENT_KIND_COMMAND    = EventKind("command")
	EVENT_KIND_DELETE_KEY = EventKind("delete-key")
)

type Event struct {
	Kind     EventKind
	Priority EventPriority
	Time     time.Time
	Args     any
}

type CommandEventArgs struct {
	Ctx      context.Context
	Message  []byte
	Conn     *net.Conn
	Replay   bool
	Embedded bool
}
