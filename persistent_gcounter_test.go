package percounter

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPersistentGCounter(t *testing.T) {
	filename := tempFilename()
	t.Cleanup(func() { os.Remove(filename) })
	var lastCount int64

	t.Run("one gcounter counter", func(t *testing.T) {
		c := NewPersistentGCounter("1", filename)
		c.Increment()
		c.Increment()
		c.Increment()
		lastCount = c.Value()
		assert.Equal(t, int64(3), lastCount)

		// wait for the write
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("picking up from persisted counter", func(t *testing.T) {
		c := NewPersistentGCounter("1", filename)
		c.Increment()
		c.Increment()
		c.Increment()

		assert.Equal(t, lastCount+3, c.Value())
		// wait for the write
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, lastCount+3, c.Value())
	})
}

func tempFilename() string {
	f, err := os.CreateTemp(".", "*.gcounter")
	if err != nil {
		panic(err)
	}
	fn := f.Name()
	_ = f.Close()
	return fn
}
