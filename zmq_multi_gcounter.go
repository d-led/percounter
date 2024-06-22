package percounter

import (
	"encoding/json"
	"log"
	"os"
	"path"

	"github.com/Arceliar/phony"
	"github.com/d-led/zmqcluster"
)

type ZmqMultiGcounter struct {
	phony.Inbox
	dirname  string
	identity string
	inner    map[string]*PersistentGCounter
	cluster  zmqcluster.Cluster
	observer CounterObserver
}

func NewObservableZmqMultiGcounterInCluster(identity, dirname string, cluster zmqcluster.Cluster, observer CounterObserver) *ZmqMultiGcounter {
	err := os.MkdirAll(dirname, os.ModePerm)
	if err != nil {
		panic(err)
	}
	res := &ZmqMultiGcounter{
		identity: identity,
		dirname:  dirname,
		observer: observer,
	}
	cluster.AddListenerSync(res)
	res.cluster = cluster
	res.inner = make(map[string]*PersistentGCounter)
	return res
}

func NewObservableZmqMultiGcounter(identity, dirname, bindAddr string, observer CounterObserver) *ZmqMultiGcounter {
	cluster := zmqcluster.NewZmqCluster(identity, bindAddr)
	return NewObservableZmqMultiGcounterInCluster(identity, dirname, cluster, observer)
}

func NewZmqMultiGcounterInCluster(identity, dirname string, cluster zmqcluster.Cluster) *ZmqMultiGcounter {
	return NewObservableZmqMultiGcounterInCluster(identity, dirname, cluster, &noOpCounterObserver{})
}

func NewZmqMultiGcounter(identity, dirname, bindAddr string) *ZmqMultiGcounter {
	return NewObservableZmqMultiGcounter(identity, dirname, bindAddr, &noOpCounterObserver{})
}

func (z *ZmqMultiGcounter) Start() error {
	return z.cluster.Start()
}

func (z *ZmqMultiGcounter) Stop() {
	z.cluster.Stop()
}

func (z *ZmqMultiGcounter) OnMessage(message []byte) {
	state := GCounterState{}
	err := json.Unmarshal(message, &state)
	if err != nil {
		log.Printf("%s: failed to deserialize state: %v", z.identity, err)
		return
	}
	z.MergeWith(NewGCounterFromState(state.Name, state))
}

func (z *ZmqMultiGcounter) OnNewPeerConnected(c zmqcluster.Cluster, peer string) {
	z.sendMyStateToPeer(peer)
}

func (z *ZmqMultiGcounter) UpdatePeers(peers []string) {
	z.cluster.UpdatePeers(peers)
}

func (z *ZmqMultiGcounter) Increment(name string) {
	z.Act(z, func() {
		counter := z.getOrCreateCounterSync(name)
		counter.IncrementFromActor(z)
	})
}

// callback once the inner counter state is changed
func (z *ZmqMultiGcounter) SetState(s GCounterState) {
	z.Act(z, func() {
		z.propagateStateSync(s)
	})
}

func (c *ZmqMultiGcounter) MergeWith(other GCounterStateSource) {
	c.Act(c, func() {
		counter := c.getOrCreateCounterSync(nameOrSingleton(other.GetState().Name))
		counter.MergeWith(other)
	})
}

func (c *ZmqMultiGcounter) Value(name string) int64 {
	var val int64
	phony.Block(c, func() {
		counter := c.getOrCreateCounterSync(name)
		val = counter.Value()
	})
	return val
}

func (c *ZmqMultiGcounter) GetCounter(name string) *PersistentGCounter {
	var res *PersistentGCounter
	phony.Block(c, func() {
		res = c.getOrCreateCounterSync(name)
	})
	return res
}

func (c *ZmqMultiGcounter) PersistSync() {
	for _, counter := range c.inner {
		counter.PersistSync()
	}
}

func (c *ZmqMultiGcounter) PersistOneSync(name string) {
	phony.Block(c, func() {
		counter := c.getOrCreateCounterSync(name)
		counter.PersistSync()
	})
}

func (z *ZmqMultiGcounter) getOrCreateCounterSync(name string) *PersistentGCounter {
	if counter, ok := z.inner[name]; ok {
		return counter
	}

	counter := NewPersistentGCounterWithSinkAndObserver(z.identity, z.multiCounterFilenameFor(name), z, z.observer)
	counter.inner.state.Name = name
	// to do: improve construction
	z.inner[name] = counter
	return counter
}

func (z *ZmqMultiGcounter) propagateStateSync(s GCounterState) {
	msg, err := json.Marshal(s)
	if err != nil {
		log.Printf("%s: error serializing state: %v", s.Name, err)
		return
	}
	z.cluster.BroadcastMessage(msg)
}

func (z *ZmqMultiGcounter) sendMyStateToPeer(peer string) {
	z.Act(z, func() {
		// send all counters
		for _, counter := range z.inner {
			s := counter.GetState()
			msg, err := json.Marshal(s)
			if err != nil {
				log.Printf("%s: error serializing state: %v", s.Name, err)
				return
			}
			// sent async - no error handling for now
			z.cluster.SendMessageToPeer(peer, msg)
		}
	})
}

func (z *ZmqMultiGcounter) multiCounterFilenameFor(name string) string {
	return path.Join(z.dirname, name+".gcounter")
}

func nameOrSingleton(name string) string {
	if name != "" {
		return name
	}
	return "singleton"
}
