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
	EVENT_KIND_COMMAND              = EventKind("command")
	EVENT_KIND_DELETE_KEY           = EventKind("delete-key")
	EVENT_KIND_UPDATE_KEYS_IN_CACHE = EventKind("update-keys-in-cache")
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

type DeleteKeysEventArgs struct {
	Ctx  context.Context
	Keys []string
}

type UpdateKeysInCacheEventArgs struct {
	Ctx  context.Context
	Keys []string
}
