package percounter

import (
	"log"
	"testing"
	"time"

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
		for w := 0; w < 15; w++ {
			if goroutineCount*incrementCount == c.Value() {
				// all ok
				return
			}
			log.Println("waiting for the counter to arrive at the expected value...")
			time.Sleep(100 * time.Millisecond)
		}
		assert.Equal(t, int64(goroutineCount*incrementCount), c.Value())
	})
}
