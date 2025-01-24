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
	EVENT_KIND_REWEIRE_AOF          = EventKind("rewrite-aof")
	EVENT_KIND_TTL_EVICTION         = EventKind("ttl-eviction")
	EVENT_KIND_ADJUST_MEM           = EventKind("adjust-memory")
)

type Event struct {
	Kind     EventKind     // The type of event
	Priority EventPriority // The event priority, higher priority events will be handled first
	Handler  func() error  // The event handler

	// Time is the time the event was generated, earlier events are processed first if they have the same priority
	Time time.Time
}
