package percounter

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const key1 = "key1"
const key2 = "key2"

func TestZmqMultiGcounter(t *testing.T) {
	t.Run("exchanging state changes", func(t *testing.T) {
		tempDir1 := t.TempDir()
		tempDir2 := t.TempDir()
		testObserver := newTestCounterObserver()
		c1 := NewObservableZmqMultiGcounter("1", tempDir1, "tcp://:5001", testObserver)
		defer c1.Stop()
		assert.NoError(t, c1.Start())
		// no repeated starts
		assert.Error(t, c1.Start())
		c1.Increment(key1)
		waitForMultiGcounterValueOf(t, 1, c1, key1)

		c2 := NewZmqMultiGcounter("2", tempDir2, "tcp://:5002")
		defer c2.Stop()
		assert.NoError(t, c2.Start())

		// upon c1 discovering a new peer, c2 should merge from c1
		c1.UpdatePeers([]string{"tcp://localhost:5002"})
		waitForMultiGcounterValueOf(t, 1, c2, key1)

		// until now, only the first 2 values should have been observed
		assert.Equal(t, []int64{0, 1}, testObserver.valuesSeen)

		// bidirectional connection
		c2.UpdatePeers([]string{"tcp://localhost:5001"})
		waitForMultiGcounterValueOf(t, 1, c1, key1)

		// incrementing c2 should cause c1 to converge on the same value
		c2.Increment(key1)
		waitForMultiGcounterValueOf(t, 2, c1, key1)

		// wait for persistence before deletion
		c1.PersistSync()
		c2.PersistSync()

		// now all should have been observed
		assert.Equal(t, []int64{0, 1, 2}, testObserver.valuesSeen)
	})

	t.Run("stopping the server", func(t *testing.T) {
		tempDir := t.TempDir()

		c1 := NewZmqMultiGcounter("1", tempDir, "tcp://:5001")
		assert.NoError(t, c1.Start())
		c1.PersistSync()
		c1.Stop()

		c2 := NewZmqMultiGcounter("1", tempDir, "tcp://:5001")
		defer c2.Stop()
		assert.NoError(t, c2.Start())
		c2.PersistSync()
	})

	t.Run("creating the directory if it doesn't exist", func(t *testing.T) {
		dir := "test-dir/counters"
		defer os.RemoveAll("test-dir")

		c1 := NewZmqMultiGcounter("1", dir, "tcp://:5001")
		defer c1.Stop()
		assert.NoError(t, c1.Start())
		c1.Increment(key1)
		c1.Increment(key1)
		c1.Increment(key2)
		waitForMultiGcounterValueOf(t, 2, c1, key1)
		waitForMultiGcounterValueOf(t, 1, c1, key2)
		c1.PersistSync()
	})

	t.Run("reopening the files", func(t *testing.T) {
		tempDir := t.TempDir()
		{
			c1 := NewZmqMultiGcounter("1", tempDir, "tcp://:5001")
			assert.NoError(t, c1.Start())
			c1.Increment(key1)
			c1.Increment(key1)
			c1.Increment(key2)
			waitForMultiGcounterValueOf(t, 2, c1, key1)
			waitForMultiGcounterValueOf(t, 1, c1, key2)
			c1.PersistSync()
			c1.Stop()
		}

		c1 := NewZmqMultiGcounter("1", tempDir, "tcp://:5001")
		defer c1.Stop()
		assert.NoError(t, c1.Start())
		c1.Increment(key2)
		waitForMultiGcounterValueOf(t, 2, c1, key1)
		waitForMultiGcounterValueOf(t, 2, c1, key2)
		c1.PersistSync()
	})
}

func waitForMultiGcounterValueOf(t *testing.T, expectedValue int64, c *ZmqMultiGcounter, key string) {
	for w := 0; w < 15; w++ {
		if expectedValue == c.Value(key) {
			// all ok
			return
		}
		log.Printf("waiting for the counter to arrive at the expected value of %d ...", expectedValue)
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(t, expectedValue, c.Value(key))
}
