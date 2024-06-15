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

	t.Run("merging with another counter with divergent memories", func(t *testing.T) {
		c := NewGCounterFromState("1", GCounterState{
			Peers: map[string]int64{
				"1": 1,
				"2": 1,
				"3": 2,
			},
		})
		c2 := NewGCounterFromState("2", GCounterState{
			Peers: map[string]int64{
				"1": 1,
				"2": 2,
				"3": 1,
			},
		})
		c.MergeWith(c2)
		assert.Equal(t, int64(5), c.Value())
	})
}
