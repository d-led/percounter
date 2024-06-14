package percounter

import (
	"github.com/Arceliar/phony"
)

type AsyncGCounter struct {
	phony.Inbox
	inner *GCounter
}

func NewAsyncGCounter(identity string) *AsyncGCounter {
	return &AsyncGCounter{
		inner: NewGCounter(identity),
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
