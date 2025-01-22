package pubsub

import (
	"bufio"
	"bytes"
	"sync"
)

type EmbeddedSub struct {
	mux    sync.Mutex
	buff   *bytes.Buffer
	writer *bufio.Writer
	reader *bufio.Reader
}

func NewEmbeddedSub() *EmbeddedSub {
	sub := &EmbeddedSub{
		mux:  sync.Mutex{},
		buff: bytes.NewBuffer(make([]byte, 0)),
	}
	sub.writer = bufio.NewWriter(sub.buff)
	sub.reader = bufio.NewReader(sub.buff)
	return sub
}

func (sub *EmbeddedSub) Write(p []byte) (int, error) {
	sub.mux.Lock()
	defer sub.mux.Unlock()
	n, err := sub.writer.Write(p)
	if err != nil {
		return n, err
	}
	err = sub.writer.Flush()
	return n, err
}

func (sub *EmbeddedSub) Read(p []byte) (int, error) {
	sub.mux.Lock()
	defer sub.mux.Unlock()

	chunk, err := sub.reader.ReadBytes(byte('\n'))
	n := copy(p, chunk)

	return n, err
}
