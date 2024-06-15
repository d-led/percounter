package percounter

import (
	"encoding/json"
	"log"
	"os"

	"github.com/Arceliar/phony"
)

type PersistentGCounter struct {
	phony.Inbox
	filename string
	inner    *GCounter
	sink     GCounterStateSink
}

func NewPersistentGCounter(identity, filename string) *PersistentGCounter {
	return NewPersistentGCounterWithSink(identity, filename, &noOpGcounterState{})
}

func NewPersistentGCounterWithSink(identity, filename string, sink GCounterStateSink) *PersistentGCounter {
	return &PersistentGCounter{
		inner:    NewGCounterFromState(identity, getStateFrom(filename)),
		filename: filename,
		sink:     sink,
	}
}

func (c *PersistentGCounter) Increment() {
	c.Act(c, func() {
		c.inner.Increment()
		c.persist()
	})
}

func (c *PersistentGCounter) Value() int64 {
	var val int64
	phony.Block(c, func() {
		val = c.inner.Value()
	})
	return val
}

func (c *PersistentGCounter) MergeWith(other *PersistentGCounter) {
	c.Act(c, func() {
		c.inner.MergeWith(other.inner)
		c.persist()
	})
}

func (c *PersistentGCounter) PersistSync() {
	phony.Block(c, func() {
		c.persistSync()
	})
}

func (c *PersistentGCounter) persist() {
	c.Act(c, func() {
		c.persistSync()
	})
}

func (c *PersistentGCounter) persistSync() {
	b, err := json.Marshal(c.inner.state)
	if err != nil {
		// something is not right with the setup
		panic(err)
	}
	err = os.WriteFile(c.filename, b, 0644)
	if err != nil {
		// something is not right with the setup
		panic(err)
	}
}

func getStateFrom(filename string) GCounterState {
	contents, err := os.ReadFile(filename)
	if err != nil || len(contents) == 0 {
		log.Printf("error reading %s: %v", filename, err)
		return NewGcounterState()
	}
	var res GCounterState
	err = json.Unmarshal(contents, &res)
	if err != nil {
		log.Printf("error deserializing state from %s: %v", filename, err)
		return NewGcounterState()
	}
	return res
}
