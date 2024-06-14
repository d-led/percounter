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
}
