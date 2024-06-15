package percounter

import (
	"github.com/Arceliar/phony"
)

type AsyncGCounter struct {
	phony.Inbox
	inner *GCounter
	sink  GCounterStateSink
}

func NewAsyncGCounter(identity string) *AsyncGCounter {
	return NewAsyncGCounterWithSink(identity, &noOpGcounterState{})
}

func NewAsyncGCounterWithSink(identity string, sink GCounterStateSink) *AsyncGCounter {
	return &AsyncGCounter{
		inner: NewGCounter(identity),
		sink:  sink,
	}
}

func NewAsyncGCounterWithSinkFromState(identity string, state GCounterState, sink GCounterStateSink) *AsyncGCounter {
	return &AsyncGCounter{
		inner: NewGCounterFromState(identity, state),
		sink:  sink,
	}
}

func (c *AsyncGCounter) Increment() {
	c.Act(c, func() {
		c.inner.Increment()
	})
}

func (c *AsyncGCounter) Value() int64 {
	var val int64
	phony.Block(c, func() {
		val = c.inner.Value()
	})
	return val
}
