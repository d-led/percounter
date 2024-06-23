package percounter

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/d-led/zmqcluster"
	"github.com/stretchr/testify/assert"
)

const name1 = "name1"
const name2 = "name2"

func TestZmqMultiGcounter(t *testing.T) {
	t.Run("exchanging state changes", func(t *testing.T) {
		tempDir1 := t.TempDir()
		tempDir2 := t.TempDir()
		port1 := randomPort()
		port2 := randomPort()
		testObserver := newTestCounterObserver()
		clusterObserver1 := newTestClusterObserver()
		c1 := NewObservableZmqMultiGcounter("1", tempDir1, "tcp://:"+port1, testObserver)
		c1.SetClusterObserver(clusterObserver1)
		defer c1.Stop()
		assert.NoError(t, c1.Start())
		// no starts are idempotent
		assert.NoError(t, c1.Start())
		c1.Increment(name1)
		waitForMultiGcounterValueOf(t, 1, c1, name1)
		// no peers yet
		assert.Len(t, clusterObserver1.MessagesReceived(), 0)

		clusterObserver2 := newTestClusterObserver()
		c2 := NewZmqMultiGcounter("2", tempDir2, "tcp://:"+port2)
		c2.SetClusterObserver(clusterObserver2)
		defer c2.Stop()
		assert.NoError(t, c2.Start())

		// upon c1 discovering a new peer, c2 should merge from c1
		c1.UpdatePeers([]string{"tcp://localhost:" + port2})
		waitForMultiGcounterValueOf(t, 1, c2, name1)
		assert.Len(t, clusterObserver1.MessagesSent(), 1)
		assert.Len(t, clusterObserver2.MessagesReceived(), 1)

		// until now, only the first 2 values should have been observed
		assert.Equal(t, []CountEvent{
			{name1, 0},
			{name1, 1},
		}, testObserver.GtValuesSeen())

		// bidirectional connection
		c2.UpdatePeers([]string{"tcp://localhost:" + port1})
		waitForMultiGcounterValueOf(t, 1, c1, name1)
		waitForMultiGcounterValueOf(t, 1, c2, name1)

		// incrementing c2 should cause c1 to converge on the same value
		c2.Increment(name1)
		waitForMultiGcounterValueOf(t, 2, c1, name1)

		// wait for persistence before deletion
		c1.PersistSync()
		c2.PersistSync()

		// now all should have been observed
		assert.Equal(t, []CountEvent{
			{name1, 0},
			{name1, 1},
			{name1, 2},
		}, testObserver.GtValuesSeen())
	})

	t.Run("stopping the server", func(t *testing.T) {
		tempDir := t.TempDir()
		port1 := randomPort()

		c1 := NewZmqMultiGcounter("1", tempDir, "tcp://:"+port1)
		assert.NoError(t, c1.Start())
		c1.PersistSync()
		c1.Stop()

		c2 := NewZmqMultiGcounter("1", tempDir, "tcp://:"+port1)
		defer c2.Stop()
		assert.NoError(t, c2.Start())
		c2.PersistSync()
	})

	t.Run("creating the directory if it doesn't exist", func(t *testing.T) {
		port1 := randomPort()

		dir := "test-dir/counters"
		defer os.RemoveAll("test-dir")

		c1 := NewZmqMultiGcounter("1", dir, "tcp://:"+port1)
		defer c1.Stop()
		assert.NoError(t, c1.Start())
		c1.Increment(name1)
		c1.Increment(name1)
		c1.Increment(name2)
		waitForMultiGcounterValueOf(t, 2, c1, name1)
		waitForMultiGcounterValueOf(t, 1, c1, name2)
		c1.PersistSync()
	})

	t.Run("reopening the files", func(t *testing.T) {
		port1 := randomPort()

		tempDir := t.TempDir()
		{
			c1 := NewZmqMultiGcounter("1", tempDir, "tcp://:"+port1)
			assert.NoError(t, c1.Start())
			c1.Increment(name1)
			c1.Increment(name1)
			c1.Increment(name2)
			waitForMultiGcounterValueOf(t, 2, c1, name1)
			waitForMultiGcounterValueOf(t, 1, c1, name2)
			c1.PersistSync()
			c1.Stop()
		}

		c1 := NewZmqMultiGcounter("1", tempDir, "tcp://:"+port1)
		defer c1.Stop()
		assert.NoError(t, c1.Start())
		c1.Increment(name2)
		waitForMultiGcounterValueOf(t, 2, c1, name1)
		waitForMultiGcounterValueOf(t, 2, c1, name2)
		c1.PersistSync()
	})

	t.Run("eagerly loading and notifying the initial state of all counters", func(t *testing.T) {
		port1 := randomPort()

		tempDir := t.TempDir()
		{
			c1 := NewZmqMultiGcounter("1", tempDir, "tcp://:"+port1)
			assert.NoError(t, c1.Start())
			c1.Increment(name1)
			c1.Increment(name1)
			c1.Increment(name2)
			waitForMultiGcounterValueOf(t, 2, c1, name1)
			waitForMultiGcounterValueOf(t, 1, c1, name2)
			c1.PersistSync()
			c1.Stop()
		}

		testObserver := newTestCounterObserver()
		c1 := NewObservableZmqMultiGcounter("1", tempDir, "tcp://:"+port1, testObserver)
		defer c1.Stop()
		assert.NoError(t, c1.Start())

		testObserver.WaitForGtValuesSeen(t, 0)

		assert.Len(t, c1.inner, 0)

		c1.LoadAllSync()

		assert.Len(t, c1.inner, 2)

		c1.PersistSync()
	})

	t.Run("multiple counters", func(t *testing.T) {
		port1 := randomPort()
		tempDir := t.TempDir()
		testObserver := newTestCounterObserver()
		c := NewObservableZmqMultiGcounter("1", tempDir, "tcp://:"+port1, testObserver)
		defer c.Stop()
		assert.NoError(t, c.Start())

		c.Increment(name1)
		c.Increment(name2)
		c.Increment(name1)
		waitForMultiGcounterValueOf(t, 2, c, name1)
		waitForMultiGcounterValueOf(t, 1, c, name2)

		assert.ElementsMatch(t, []CountEvent{
			{name1, 0},
			{name1, 1},
			{name1, 2},
			{name2, 0},
			{name2, 1},
		}, testObserver.WaitForGtValuesSeen(t, 5))

		c.PersistSync()
	})

	t.Run("re-using an existing cluster but the named counters are not synced", func(t *testing.T) {
		port1 := randomPort()
		c := zmqcluster.NewZmqCluster("1", "tcp://:"+port1)
		t.Cleanup(c.Stop)

		tempDir1 := t.TempDir()
		c1 := NewZmqMultiGcounterInCluster("1", tempDir1, c)
		c.AddListenerSync(c1)
		assert.NoError(t, c1.Start())

		tempDir2 := t.TempDir()
		t.Log("!!!do not do this yet!!! do not share the cluster for two ZmqMultiGcounter")
		c2 := NewZmqMultiGcounterInCluster("2", tempDir2, c)
		assert.NoError(t, c2.Start())

		c1.Increment(name1) //1
		c1.Increment(name2)
		c1.Increment(name2) //2

		c2.Increment(name1)
		c2.Increment(name2)
		c2.Increment(name2)
		c2.Increment(name1)
		c2.Increment(name2)
		c2.Increment(name1) //3
		c2.Increment(name2) //4

		waitForMultiGcounterValueOf(t, 4, c2, name2)
		waitForMultiGcounterValueOf(t, 3, c2, name1)

		waitForMultiGcounterValueOf(t, 2, c1, name2)
		waitForMultiGcounterValueOf(t, 1, c1, name1)

		c1.PersistSync()
		c2.PersistSync()

		// here be dragons! c.UpdatePeers(...)
	})
}

func waitForMultiGcounterValueOf(t *testing.T, expectedValue int64, c *ZmqMultiGcounter, name string) {
	for w := 0; w < 15; w++ {
		if expectedValue == c.Value(name) {
			// all ok
			return
		}
		log.Printf("waiting for the counter to arrive at the expected value of %d ...", expectedValue)
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(t, expectedValue, c.Value(name))
}
