package percounter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPersistentGCounter(t *testing.T) {
	t.Run("one gcounter counter", func(t *testing.T) {
		filename := newTempFilename(t)
		c := NewPersistentGCounter("1", filename)
		c.Increment()
		c.Increment()
		c.Increment()
		waitForGcounterValueOf(t, 3, c)
		c.PersistSync()
	})

	t.Run("picking up from persisted counter", func(t *testing.T) {
		filename := newTempFilename(t)
		{
			c := NewPersistentGCounter("1", filename)
			c.Increment()
			c.Increment()
			c.Increment()
			waitForGcounterValueOf(t, 3, c)
			c.PersistSync()
		}

		c := NewPersistentGCounter("1", filename)
		c.Increment()
		c.Increment()
		c.Increment()
		waitForGcounterValueOf(t, 6, c)
		c.PersistSync()
	})

	t.Run("merging with another counter", func(t *testing.T) {
		filename := newTempFilename(t)
		c := NewPersistentGCounter("1", filename)
		c.Increment()
		c.Increment()
		waitForGcounterValueOf(t, 2, c)
		c.PersistSync()

		filename2 := newTempFilename(t)
		c2 := NewPersistentGCounter("2", filename2)
		c2.Increment()
		waitForGcounterValueOf(t, 1, c2)
		c2.PersistSync()

		c.MergeWith(c2)

		waitForGcounterValueOf(t, 3, c)
		c.PersistSync()
	})

	t.Run("observing state change", func(t *testing.T) {
		filename := newTempFilename(t)
		testsink := &testGCounterStateSink{}
		c := NewPersistentGCounterWithSink("1", filename, testsink)
		c.Increment()
		waitForGcounterValueOf(t, 1, c)
		assert.Equal(t, int64(1), testsink.lastState.Peers["1"])

		c.Increment()
		c.Increment()
		waitForGcounterValueOf(t, 3, c)
		assert.Equal(t, int64(3), testsink.lastState.Peers["1"])
		c.PersistSync()
	})

	t.Run("observing counter value", func(t *testing.T) {
		filename := newTempFilename(t)
		testObserver := newTestCounterObserver()
		c := NewPersistentGCounterWithSinkAndObserver("1", filename, &noOpGcounterState{}, testObserver)

		c.Increment()
		waitForGcounterValueOf(t, 1, c)

		// let some goroutines run
		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, []int64{0, 1}, testObserver.valuesSeen)

		c.MergeWith(NewGCounterFromState("2", GCounterState{Peers: map[string]int64{"1": 1}}))
		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, []int64{0, 1}, testObserver.valuesSeen)

		c.MergeWith(NewGCounterFromState("2", GCounterState{Peers: map[string]int64{"2": 1}}))
		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, []int64{0, 1, 2}, testObserver.valuesSeen)

		c.PersistSync()
	})
}
