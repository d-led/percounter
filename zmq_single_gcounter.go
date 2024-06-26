package percounter

import (
	"encoding/json"
	"log"

	"github.com/Arceliar/phony"
	"github.com/d-led/zmqcluster"
)

type ZmqSingleGcounter struct {
	phony.Inbox
	inner   *PersistentGCounter
	cluster zmqcluster.Cluster
}

func NewZmqSingleGcounterInCluster(identity, filename string, cluster zmqcluster.Cluster) *ZmqSingleGcounter {
	res := &ZmqSingleGcounter{}
	cluster.AddListenerSync(res)
	res.cluster = cluster
	res.inner = NewPersistentGCounterWithSink(identity, filename, res)
	return res
}

func NewObservableZmqSingleGcounter(identity, filename, bindAddr string, observer CounterObserver) *ZmqSingleGcounter {
	res := &ZmqSingleGcounter{}
	cluster := zmqcluster.NewZmqCluster(identity, bindAddr)
	cluster.AddListenerSync(res)
	res.cluster = cluster
	res.inner = NewPersistentGCounterWithSinkAndObserver(identity, filename, res, observer)
	return res
}

func NewZmqSingleGcounter(identity, filename, bindAddr string) *ZmqSingleGcounter {
	res := &ZmqSingleGcounter{}
	cluster := zmqcluster.NewZmqCluster(identity, bindAddr)
	cluster.AddListenerSync(res)
	res.cluster = cluster
	res.inner = NewPersistentGCounterWithSink(identity, filename, res)
	return res
}

func (z *ZmqSingleGcounter) Start() error {
	return z.cluster.Start()
}

func (z *ZmqSingleGcounter) Stop() {
	z.cluster.Stop()
}

func (z *ZmqSingleGcounter) OnMessage(_ []byte, message []byte) {
	state := GCounterState{}
	err := json.Unmarshal(message, &state)
	if err != nil {
		log.Printf("%s: failed to deserialize state: %v", z.inner.inner.identity, err)
		return
	}
	z.MergeWith(NewGCounterFromState("temporary-counter", state))
}

func (z *ZmqSingleGcounter) OnNewPeerConnected(c zmqcluster.Cluster, peer string) {
	z.sendMyStateToPeer(peer)
}

func (z *ZmqSingleGcounter) UpdatePeers(peers []string) {
	z.cluster.UpdatePeers(peers)
}

func (z *ZmqSingleGcounter) Increment() {
	z.Act(z, func() {
		z.inner.Increment()
	})
}

// callback once the inner counter state is changed
func (z *ZmqSingleGcounter) SetState(s GCounterState) {
	z.Act(z, func() {
		z.propagateStateSync(s)
	})
}

func (c *ZmqSingleGcounter) MergeWith(other GCounterStateSource) {
	c.Act(c, func() {
		c.inner.MergeWith(other)
	})
}

func (c *ZmqSingleGcounter) Value() int64 {
	var val int64
	phony.Block(c, func() {
		val = c.inner.Value()
	})
	return val
}

func (c *ZmqSingleGcounter) PersistSync() {
	c.inner.PersistSync()
}

func (z *ZmqSingleGcounter) propagateStateSync(s GCounterState) {
	msg, err := json.Marshal(s)
	if err != nil {
		log.Printf("%s: error serializing state: %v", s.Name, err)
		return
	}
	z.cluster.BroadcastMessage(msg)
}

func (z *ZmqSingleGcounter) sendMyStateToPeer(peer string) {
	z.Act(z, func() {
		s := z.inner.GetState()
		msg, err := json.Marshal(s)
		if err != nil {
			log.Printf("%s: error serializing state: %v", s.Name, err)
			return
		}
		// sent async - no error handling for now
		z.cluster.SendMessageToPeer(peer, msg)
	})
}
