package percounter

import (
	"encoding/json"
	"log"
	"os"
	"path"
	"strings"

	"github.com/Arceliar/phony"
)

type PersistentGCounter struct {
	phony.Inbox
	filename          string
	inner             *GCounter
	sink              GCounterStateSink
	observer          CounterObserver
	lastObservedCount int64
}

func NewPersistentGCounter(identity, filename string) *PersistentGCounter {
	return NewPersistentGCounterWithSink(identity, filename, &noOpGcounterState{})
}

func NewPersistentGCounterWithSink(identity, filename string, sink GCounterStateSink) *PersistentGCounter {
	res := &PersistentGCounter{
		inner:    NewGCounterFromState(identity, getStateFrom(filename)),
		filename: filename,
		sink:     sink,
		observer: &noOpCounterObserver{},
	}
	res.lastObservedCount = res.inner.Value()
	return res
}

func NewPersistentGCounterWithSinkAndObserver(identity, filename string, sink GCounterStateSink, observer CounterObserver) *PersistentGCounter {
	res := &PersistentGCounter{
		inner:    NewGCounterFromState(identity, getStateFrom(filename)),
		filename: filename,
		sink:     sink,
		observer: observer,
	}
	observer.OnNewCount(CountEvent{res.inner.state.Name, res.inner.Value()})
	res.lastObservedCount = res.inner.Value()
	return res
}

func (c *PersistentGCounter) Increment() {
	c.IncrementFromActor(c)
}

func (c *PersistentGCounter) IncrementFromActor(anotherActor phony.Actor) {
	c.Act(anotherActor, func() {
		c.inner.Increment()
		c.publishCountIfChangedSync()
		c.sink.SetState(c.inner.GetState())
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

func (c *PersistentGCounter) GetState() GCounterState {
	var res GCounterState
	phony.Block(c, func() {
		res = c.inner.GetState()
	})
	return res
}

func (c *PersistentGCounter) MergeWith(other GCounterStateSource) {
	c.Act(c, func() {
		c.inner.MergeWith(other)
		c.publishCountIfChanged()
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

func (c *PersistentGCounter) publishCountIfChangedSync() {
	newCount := c.inner.Value()
	if newCount != c.lastObservedCount {
		name := c.inner.state.Name
		c.observer.OnNewCount(CountEvent{name, newCount})
		c.lastObservedCount = newCount
	}
}

func (c *PersistentGCounter) publishCountIfChanged() {
	c.Act(c, func() {
		c.publishCountIfChangedSync()
	})
}

func (c *PersistentGCounter) persistSync() {
	b, err := json.Marshal(c.inner.GetState())
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
	counterName := getFilenameWithoutExtension(filename)
	contents, err := os.ReadFile(filename)
	if err != nil || len(contents) == 0 {
		log.Printf("error reading %s: %v", filename, err)
		return NewNamedGcounterState(counterName)
	}
	var res GCounterState
	err = json.Unmarshal(contents, &res)
	if err != nil {
		log.Printf("error deserializing state from %s: %v", filename, err)
		return NewNamedGcounterState(counterName)
	}
	if res.Name == "" {
		res.Name = counterName
	}
	return res
}

func getFilenameWithoutExtension(filename string) string {
	withoutDir := path.Base(filename)
	return strings.TrimSuffix(withoutDir, path.Ext(withoutDir))
}
