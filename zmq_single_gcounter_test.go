package percounter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZmqSingleGcounter(t *testing.T) {
	t.Run("exchanging state changes", func(t *testing.T) {
		f1 := newTempFilename(t)
		f2 := newTempFilename(t)
		testObserver := newTestCounterObserver()
		c1 := NewObservableZmqSingleGcounter("1", f1, "tcp://:5001", testObserver)
		defer c1.Stop()
		assert.NoError(t, c1.Start())
		// starts are idempotent
		assert.NoError(t, c1.Start())
		c1.Increment()
		waitForGcounterValueOf(t, 1, c1)

		c2 := NewZmqSingleGcounter("2", f2, "tcp://:5002")
		defer c2.Stop()
		assert.NoError(t, c2.Start())

		// upon c1 discovering a new peer, c2 should merge from c1
		c1.UpdatePeers([]string{"tcp://localhost:5002"})
		waitForGcounterValueOf(t, 1, c2)

		// until now, only the first 2 values should have been observed
		assertValuesSeen(t, []int64{0, 1}, testObserver.GtValuesSeen())

		// bidirectional connection
		c2.UpdatePeers([]string{"tcp://localhost:5001"})
		waitForGcounterValueOf(t, 1, c1)

		// incrementing c2 should cause c1 to converge on the same value
		c2.Increment()
		waitForGcounterValueOf(t, 2, c1)

		// wait for persistence before deletion
		c1.PersistSync()
		c2.PersistSync()

		// now all should have been observed
		assertValuesSeen(t, []int64{0, 1, 2}, testObserver.GtValuesSeen())
	})

	t.Run("stopping the server", func(t *testing.T) {
		f := newTempFilename(t)
		c1 := NewZmqSingleGcounter("1", f, "tcp://:5001")
		assert.NoError(t, c1.Start())
		c1.PersistSync()
		c1.Stop()

		c2 := NewZmqSingleGcounter("1", f, "tcp://:5001")
		defer c2.Stop()
		assert.NoError(t, c2.Start())
		c2.PersistSync()
	})

	t.Run("re-using an existing cluster", func(t *testing.T) {
		port1 := randomPort()
		c := NewZmqCluster("1", "tcp://:"+port1)
		t.Cleanup(c.Stop)

		f := newTempFilename(t)
		c1 := NewZmqSingleGcounterInCluster("1", f, c)
		c.AddListenerSync(c1)
		assert.NoError(t, c1.Start())

		c2 := NewZmqSingleGcounterInCluster("2", f, c)
		assert.NoError(t, c2.Start())

		c1.Increment()
		c2.Increment()
		c2.Increment()

		waitForGcounterValueOf(t, 2, c2)
		waitForGcounterValueOf(t, 1, c1)

		c1.PersistSync()
		c2.PersistSync()
	})
}

func assertValuesSeen(t *testing.T, expected []int64, events []CountEvent) {
	require.Len(t, events, len(expected))
	for pos, e := range expected {
		assert.Equal(t, e, events[pos].Count)
	}
}
