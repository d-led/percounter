package percounter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGCounter(t *testing.T) {
	t.Run("one gcounter counter", func(t *testing.T) {
		c := NewGCounter("1")
		c.Increment()
		c.Increment()
		c.Increment()
		assert.Equal(t, int64(3), c.Value())
	})

	t.Run("merging with another counter", func(t *testing.T) {
		c := NewGCounter("1")
		c.Increment()
		c.Increment()
		c2 := NewGCounter("2")
		c2.Increment()
		c.MergeWith(c2)
		assert.Equal(t, int64(3), c.Value())
	})
}
