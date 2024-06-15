package percounter

import (
	"testing"
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
}
