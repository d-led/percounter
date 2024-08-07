package percounter

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"

	"github.com/Arceliar/phony"
	"github.com/d-led/zmqcluster"
)

type ZmqMultiGcounter struct {
	phony.Inbox
	dirname               string
	identity              string
	peers                 []string //for tracing only
	inner                 map[string]*PersistentGCounter
	cluster               zmqcluster.Cluster
	observer              CounterObserver
	clusterObserver       ClusterObserver
	shouldPersistOnSignal bool
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
		peers:    []string{},
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

func (z *ZmqMultiGcounter) SetClusterObserver(o ClusterObserver) {
	phony.Block(z, func() {
		z.clusterObserver = o
	})
}

func (z *ZmqMultiGcounter) ShouldPersistOnSignal() {
	phony.Block(z, func() {
		z.shouldPersistOnSignal = true
	})
}

func (z *ZmqMultiGcounter) LoadAllSync() error {
	var err error
	phony.Block(z, func() {
		var files []fs.DirEntry
		files, err = os.ReadDir(z.dirname)
		if err != nil {
			return
		}
		for _, f := range files {
			counterName, ok := getCounterName(f.Name())
			if !ok {
				continue
			}
			_ = z.getOrCreateCounterSync(counterName)
		}
	})
	return err
}

func (z *ZmqMultiGcounter) Start() error {
	return z.cluster.Start()
}

func (z *ZmqMultiGcounter) Stop() {
	z.cluster.Stop()
}

func (z *ZmqMultiGcounter) OnMessage(identity []byte, message []byte) {
	state := NetworkedGCounterState{}
	err := json.Unmarshal(message, &state)
	if err != nil {
		log.Printf("%s: failed to deserialize state: %v", z.identity, err)
		return
	}
	switch state.Type {
	case GCounterNetworkMessage:
		z.MergeWith(NewGCounterFromState(state.Name, GCounterState{state.Name, state.Peers}))
	case PeerOhaiNetworkMessage:
		log.Printf("received an 'ohai' from %s, sending 'hello' back", string(identity))
		peerIp, err := tryGetPeerIp(&state)
		if err != nil {
			log.Printf("Error extracting peer IP from 'ohai': %v", err)
			break
		}
		peerPort, err := tryGetPeerTcpPort(&state)
		if err != nil {
			log.Printf("Error extracting peer TCP port from 'ohai': %v", err)
			break
		}
		if peerIp != "" && peerPort != "" {
			log.Println("sending 'hello' to", peerIp, err)
			z.sendHelloToPeer(zmqAddressOf(peerIp, peerPort))
		}
	case PeerHelloNetworkMessage:
		log.Printf("received a 'hello' from %s", string(identity))
	default:
		log.Printf("unknown message type '%s' received: name:'%s', source_peer:'%s', ignoring", state.Type, state.Name, state.SourcePeer)
		return
	}

	peer := string(identity)

	if len(identity) == 0 {
		peer = state.SourcePeer
	}

	if z.clusterObserver != nil {
		z.clusterObserver.AfterMessageReceived(peer, message)
	}
}

func (z *ZmqMultiGcounter) OnMessageSent(peer string, message []byte) {
	if z.clusterObserver != nil {
		z.clusterObserver.AfterMessageSent(peer, message)
	}
}

func (z *ZmqMultiGcounter) OnNewPeerConnected(c zmqcluster.Cluster, peer string) {
	z.sendMyStateToPeer(peer)
}

func (z *ZmqMultiGcounter) UpdatePeers(peers []string) {
	z.Act(z, func() {
		z.cluster.UpdatePeers(peers)
		z.peers = peers
		z.broadcastOhaiSync()
	})
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
	phony.Block(c, func() {
		for _, counter := range c.inner {
			counter.PersistSync()
		}
	})
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
	if z.shouldPersistOnSignal {
		GlobalEmergencyPersistence().AddForPersistence(counter)
	}
	return counter
}

func (z *ZmqMultiGcounter) propagateStateSync(s GCounterState) {
	networkedState := NetworkedGCounterState{
		Type:       GCounterNetworkMessage,
		SourcePeer: z.identity,
		Name:       s.Name,
		Peers:      s.Peers,
		Metadata:   z.myConnectionInfoSync(),
	}
	msg, err := json.Marshal(networkedState)
	if err != nil {
		log.Printf("%s: error serializing state: %v", s.Name, err)
		return
	}
	z.cluster.BroadcastMessage(msg)
	if z.clusterObserver == nil {
		return
	}
	for _, peer := range z.peers {
		z.clusterObserver.AfterMessageSent(peer, msg)
	}
}

func (z *ZmqMultiGcounter) sendMyStateToPeer(peer string) {
	z.Act(z, func() {
		// send all counters
		for _, counter := range z.inner {
			s := counter.GetState()
			networkedState := NetworkedGCounterState{
				Type:       GCounterNetworkMessage,
				SourcePeer: z.identity,
				Name:       s.Name,
				Peers:      s.Peers,
				Metadata:   z.myConnectionInfoSync(),
			}
			msg, err := json.Marshal(networkedState)
			if err != nil {
				log.Printf("%s: error serializing state: %v", s.Name, err)
				return
			}
			// sent async - no error handling for now
			z.cluster.SendMessageToPeer(peer, msg)
			if z.clusterObserver != nil {
				z.clusterObserver.AfterMessageSent(peer, msg)
			}
		}
	})
}

func (z *ZmqMultiGcounter) broadcastOhaiSync() {
	ohai := NetworkedGCounterState{
		Type:       PeerOhaiNetworkMessage,
		SourcePeer: z.identity,
		Metadata:   z.myConnectionInfoSync(),
	}
	msg, err := json.Marshal(ohai)
	if err != nil {
		log.Println("error serializing ohai: ", err)
		return
	}
	z.cluster.BroadcastMessage(msg)
}

func (z *ZmqMultiGcounter) sendHelloToPeer(peer string) {
	ohai := NetworkedGCounterState{
		Type:       PeerHelloNetworkMessage,
		SourcePeer: z.identity,
		Metadata:   map[string]interface{}{"my_ip": z.cluster.MyIP()},
	}
	msg, err := json.Marshal(ohai)
	if err != nil {
		log.Println("error serializing hello: ", err)
		return
	}
	z.cluster.SendMessageToPeer(peer, msg)
}

func zmqAddressOf(peerIp, peerPort string) string {
	return fmt.Sprintf("tcp://[%s]:%s", peerIp, peerPort)
}

func tryGetPeerIp(msg *NetworkedGCounterState) (string, error) {
	return tryGetPeerMetadataString(msg, MyIPKey)
}

func tryGetPeerTcpPort(msg *NetworkedGCounterState) (string, error) {
	return tryGetPeerMetadataString(msg, MyTcpPortKey)
}

func tryGetPeerMetadataString(msg *NetworkedGCounterState, key string) (string, error) {
	valI, ok := msg.Metadata[key]
	if !ok {
		return "", fmt.Errorf("no field '%s' in message", key)
	}
	val, ok := valI.(string)
	if !ok {
		return "", fmt.Errorf("'%s' is not of type 'string'", key)
	}
	return val, nil
}

func (z *ZmqMultiGcounter) multiCounterFilenameFor(name string) string {
	return path.Join(z.dirname, name+".gcounter")
}

func (z *ZmqMultiGcounter) myConnectionInfoSync() map[string]interface{} {
	return map[string]interface{}{
		MyIPKey:      z.cluster.MyIP(),
		MyTcpPortKey: z.cluster.MyTcpPort(),
	}
}

func nameOrSingleton(name string) string {
	if name != "" {
		return name
	}
	return "singleton"
}

func getCounterName(filename string) (string, bool) {
	if filepath.Ext(filename) != ".gcounter" {
		return "", false
	}
	return getFilenameWithoutExtension(filename), true
}
