package percounter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAsyncGCounter(t *testing.T) {
	t.Run("one gcounter counter", func(t *testing.T) {
		c := NewAsyncGCounter("1")
		c.Increment()
		c.Increment()
		c.Increment()
		assert.Equal(t, int64(3), c.Value())
	})

	t.Run("merging with another counter", func(t *testing.T) {
		c := NewAsyncGCounter("1")
		c.Increment()
		c.Increment()
		c2 := NewAsyncGCounter("2")
		c2.Increment()
		c.MergeWith(c2)
		assert.Equal(t, int64(3), c.Value())
	})

	t.Run("eventual consistency in-process", func(t *testing.T) {
		c := NewAsyncGCounter("1")
		const goroutineCount = 32
		const incrementCount = 1000
		for i := 0; i < goroutineCount; i++ {
			go func() {
				for j := 0; j < incrementCount; j++ {
					c.Increment()
				}
			}()
		}
		waitForGcounterValueOf(t, goroutineCount*incrementCount, c)
	})
}
