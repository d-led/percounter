package percounter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/Arceliar/phony"
	"github.com/go-zeromq/zmq4"
)

type ZmqMultiGcounter struct {
	phony.Inbox
	dirname  string
	identity string
	inner    map[string]*PersistentGCounter
	bindAddr string
	observer CounterObserver
	server   zmq4.Socket
	peers    map[string]zmq4.Socket
	started  bool
	ctx      context.Context
	stop     context.CancelFunc
}

func NewObservableZmqMultiGcounter(identity, dirname, bindAddr string, observer CounterObserver) *ZmqMultiGcounter {
	err := os.MkdirAll(dirname, os.ModePerm)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	res := &ZmqMultiGcounter{
		identity: identity,
		dirname:  dirname,
		bindAddr: bindAddr,
		observer: observer,
		server:   zmq4.NewPull(ctx),
		peers:    make(map[string]zmq4.Socket),
		ctx:      ctx,
		stop:     cancel,
	}
	res.inner = make(map[string]*PersistentGCounter)
	return res
}

func NewZmqMultiGcounter(identity, dirname, bindAddr string) *ZmqMultiGcounter {
	return NewObservableZmqMultiGcounter(identity, dirname, bindAddr, &noOpCounterObserver{})
}

func (z *ZmqMultiGcounter) Start() error {
	var err error
	phony.Block(z, func() {
		if z.started {
			err = errors.New(z.identity + " already started")
		}
		err = z.server.Listen(z.bindAddr)
		if err != nil {
			err = fmt.Errorf("could not start listening at %s: %v", z.bindAddr, err)
			return
		}
		z.started = true
		go z.receiveLoop()
	})
	return err
}

func (z *ZmqMultiGcounter) Stop() {
	z.stop()
	phony.Block(z, func() {
		log.Printf("%s: disconnecting", z.identity)
		z.server.Close()
	})
}

func (z *ZmqMultiGcounter) UpdatePeers(peers []string) {
	z.Act(z, func() {
		newPeers := setOf(peers)

		// if not in new peers, close & remove the connection
		for clientPeer, conn := range z.peers {
			if _, ok := newPeers[clientPeer]; !ok {
				log.Println("Removing connection to", clientPeer)
				conn.Close()
				delete(z.peers, clientPeer)
			}
		}

		// if not connected, connect
		for _, peer := range peers {
			if _, ok := z.peers[peer]; !ok {
				socket := zmq4.NewPush(context.Background())
				log.Println(z.identity+": connecting to", peer)
				err := socket.Dial(peer)
				if err != nil {
					log.Printf(z.identity+": could not connect to peer: %s: %v", peer, err)
					continue
				}
				z.peers[peer] = socket
				z.sendMyStateToPeerSync(peer)
			}
		}
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

func (z *ZmqMultiGcounter) receiveLoop() {
	log.Printf(z.identity+": started listening to incoming messages at %s", z.bindAddr)
	for {
		select {
		case <-z.ctx.Done():
			log.Printf("%s: stopped listening to incoming messages at %s", z.identity, z.bindAddr)
			z.started = false
			return
		default:
		}

		msg, err := z.server.Recv()
		if err != nil {
			log.Printf("%s: failed receive: %v", z.identity, err)
			if !errors.Is(err, context.Canceled) {
				go z.receiveLater()
			} else {
				log.Printf("%s: stopping receive loop", z.identity)
			}
			return
		}
		state := GCounterState{}
		err = json.Unmarshal(msg.Bytes(), &state)
		if err != nil {
			log.Printf("%s: failed to deserialize state: %v", z.identity, err)
			go z.receiveLater()
			return
		}
		z.MergeWith(NewGCounterFromState(state.Name, state))
	}
}

func (z *ZmqMultiGcounter) receiveLater() {
	time.Sleep(100 * time.Millisecond)
	z.receiveLoop()
}

func (z *ZmqMultiGcounter) propagateStateSync(s GCounterState) {
	for _, client := range z.peers {
		sendStateToPeerSync(client, s)
	}
}

func (z *ZmqMultiGcounter) sendMyStateToPeerSync(peer string) {
	client, ok := z.peers[peer]
	if !ok {
		log.Printf("%s: client not found for peer %s", z.identity, peer)
		return
	}

	// send all counters
	for _, counter := range z.inner {
		sendStateToPeerSync(client, counter.GetState())
	}
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

func sendStateToPeerSync(c zmq4.Socket, s GCounterState) {
	msg, err := json.Marshal(s)
	if err != nil {
		log.Printf("%s: error serializing state: %v", s.Name, err)
		return
	}

	err = c.Send(zmq4.NewMsg(msg))
	if err != nil {
		log.Printf("%s: error sending state to peer: %v", s.Name, err)
		return
	}
}
