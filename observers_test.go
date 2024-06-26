package percounter

import (
	"log"
	"testing"
	"time"

	"github.com/Arceliar/phony"
	"github.com/stretchr/testify/assert"
)

type testMessageEvent struct {
	peer string
	msg  string
}

type testCounterObserver struct {
	phony.Inbox
	valuesSeen []CountEvent
}

func newTestCounterObserver() *testCounterObserver {
	return &testCounterObserver{
		valuesSeen: []CountEvent{},
	}
}

func (o *testCounterObserver) OnNewCount(c CountEvent) {
	o.Act(o, func() {
		o.valuesSeen = append(o.valuesSeen, c)
	})
}

func (o *testCounterObserver) GtValuesSeen() []CountEvent {
	var res []CountEvent
	phony.Block(o, func() {
		res = append(res, o.valuesSeen...)
	})
	return res
}

func (o *testCounterObserver) WaitForGtValuesSeen(t *testing.T, expectedCount int) []CountEvent {
	for w := 0; w < 15; w++ {
		if expectedCount == len(o.GtValuesSeen()) {
			// all ok
			return o.GtValuesSeen()
		}
		log.Printf("waiting for the event count to arrive at the expected value of %d ...", expectedCount)
		time.Sleep(100 * time.Millisecond)
	}
	res := o.GtValuesSeen()
	assert.Equal(t, expectedCount, len(res))
	return res
}

type testClusterObserver struct {
	phony.Inbox
	messagesSent     []testMessageEvent
	messagesReceived []testMessageEvent
}

func newTestClusterObserver() *testClusterObserver {
	return &testClusterObserver{
		messagesSent:     []testMessageEvent{},
		messagesReceived: []testMessageEvent{},
	}
}

func (o *testClusterObserver) AfterMessageSent(peer string, msg []byte) {
	o.Act(o, func() {
		o.messagesSent = append(o.messagesSent, testMessageEvent{peer, string(msg)})
	})
}

func (o *testClusterObserver) AfterMessageReceived(peer string, msg []byte) {
	o.Act(o, func() {
		o.messagesReceived = append(o.messagesReceived, testMessageEvent{peer, string(msg)})
	})
}

func (o *testClusterObserver) MessagesReceived() []testMessageEvent {
	var res []testMessageEvent
	phony.Block(o, func() {
		res = append(res, o.messagesReceived...)
	})
	return res
}

func (o *testClusterObserver) MessagesSent() []testMessageEvent {
	var res []testMessageEvent
	phony.Block(o, func() {
		res = append(res, o.messagesSent...)
	})
	return res
}
