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

package sugardb

import (
	"bufio"
	"bytes"
	"github.com/echovault/sugardb/internal"
	"github.com/tidwall/resp"
	"strings"
	"sync"
)

type messageBuffer struct {
	buff       []byte
	buffer     *bytes.Buffer
	buffWriter *bufio.Writer
	buffReader *bufio.Reader
}

var subscriptions sync.Map

// Subscribe subscribes the caller to the list of provided channels.
//
// Parameters:
//
// `tag` - string - The tag used to identify this subscription instance.
//
// `channels` - ...string - The list of channels to subscribe to.
//
// Returns: ReadPubSubMessage function which reads the next message sent to the subscription instance.
// This function is blocking.
func (server *SugarDB) Subscribe(tag string, channels ...string) (*bufio.Reader, error) {
	var msgBuffer *messageBuffer

	sub, ok := subscriptions.Load(tag)
	if !ok {
		// Create new messageBuffer and store it in the subscriptions
		msgBuffer = &messageBuffer{
			buff: make([]byte, 0),
		}
		msgBuffer.buffer = bytes.NewBuffer(msgBuffer.buff)
		msgBuffer.buffWriter = bufio.NewWriter(msgBuffer.buffer)
		msgBuffer.buffReader = bufio.NewReader(msgBuffer.buffer)
		subscriptions.Store(tag, msgBuffer)
	} else {
		msgBuffer = sub.(*messageBuffer)
	}

	server.pubSub.Subscribe(msgBuffer.buffWriter, channels, false)

	return msgBuffer.buffReader, nil
}

// Unsubscribe unsubscribes the caller from the given channels.
//
// Parameters:
//
// `tag` - string - The tag used to identify this subscription instance.
//
// `channels` - ...string - The list of channels to unsubscribe from.
func (server *SugarDB) Unsubscribe(tag string, channels ...string) {
	sub, ok := subscriptions.Load(tag)
	if !ok {
		return
	}
	msgBuffer := sub.(*messageBuffer)
	server.pubSub.Unsubscribe(msgBuffer.buffWriter, channels, false)
}

// PSubscribe subscribes the caller to the list of provided glob patterns.
//
// Parameters:
//
// `tag` - string - The tag used to identify this subscription instance.
//
// `patterns` - ...string - The list of glob patterns to subscribe to.
//
// Returns: ReadPubSubMessage function which reads the next message sent to the subscription instance.
// This function is blocking.
func (server *SugarDB) PSubscribe(tag string, patterns ...string) (*bufio.Reader, error) {
	var msgBuffer *messageBuffer

	sub, ok := subscriptions.Load(tag)
	if !ok {
		// Create new messageBuffer and store it in the subscriptions
		msgBuffer = &messageBuffer{
			buff: make([]byte, 0),
		}
		msgBuffer.buffer = bytes.NewBuffer(msgBuffer.buff)
		msgBuffer.buffWriter = bufio.NewWriter(msgBuffer.buffer)
		msgBuffer.buffReader = bufio.NewReader(msgBuffer.buffer)
		subscriptions.Store(tag, msgBuffer)
	} else {
		msgBuffer = sub.(*messageBuffer)
	}

	server.pubSub.Subscribe(msgBuffer.buffWriter, patterns, true)

	return msgBuffer.buffReader, nil
}

// PUnsubscribe unsubscribes the caller from the given glob patterns.
//
// Parameters:
//
// `tag` - string - The tag used to identify this subscription instance.
//
// `patterns` - ...string - The list of glob patterns to unsubscribe from.
func (server *SugarDB) PUnsubscribe(tag string, patterns ...string) {
	sub, ok := subscriptions.Load(tag)
	if !ok {
		return
	}
	msgBuffer := sub.(*messageBuffer)
	server.pubSub.Unsubscribe(msgBuffer.buffWriter, patterns, true)
}

// Publish publishes a message to the given channel.
//
// Parameters:
//
// `channel` - string - The channel to publish the message to.
//
// `message` - string - The message to publish to the specified channel.
//
// Returns: true when successful. This does not indicate whether each subscriber has received the message,
// only that the message has been published to the channel.
func (server *SugarDB) Publish(channel, message string) (bool, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"PUBLISH", channel, message}), nil, false, true)
	if err != nil {
		return false, err
	}
	s, err := internal.ParseStringResponse(b)
	return strings.EqualFold(s, "ok"), err
}

// PubSubChannels returns the list of channels & patterns that match the glob pattern provided.
//
// Parameters:
//
// `pattern` - string - The glob pattern used to match the channel names.
//
// Returns: A string slice of all the active channels and patterns (i.e. channels and patterns that have 1 or more subscribers).
func (server *SugarDB) PubSubChannels(pattern string) ([]string, error) {
	cmd := []string{"PUBSUB", "CHANNELS"}
	if pattern != "" {
		cmd = append(cmd, pattern)
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// PubSubNumPat returns the list of active patterns.
//
// Returns: An integer representing the number of all the active patterns (i.e. patterns that have 1 or more subscribers).
func (server *SugarDB) PubSubNumPat() (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"PUBSUB", "NUMPAT"}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// PubSubNumSub returns the number of subscribers for each of the specified channels.
//
// Parameters:
//
// `channels` - ...string - The list of channels whose number of subscribers is to be checked.
//
// Returns: A map of map[string]int where the key is the channel name and the value is the number of subscribers.
func (server *SugarDB) PubSubNumSub(channels ...string) (map[string]int, error) {
	cmd := append([]string{"PUBSUB", "NUMSUB"}, channels...)

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	r := resp.NewReader(bytes.NewReader(b))
	v, _, err := r.ReadValue()
	if err != nil {
		return nil, err
	}

	arr := v.Array()

	result := make(map[string]int, len(arr))
	for _, entry := range arr {
		e := entry.Array()
		result[e[0].String()] = e[1].Integer()
	}

	return result, nil
}
