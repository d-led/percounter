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
			err = errors.New("already started")
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
				log.Println("Connecting to", peer)
				err := socket.Dial(peer)
				if err != nil {
					log.Printf("Could not connect to peer: %s: %v", peer, err)
					continue
				}
				z.peers[peer] = socket
				z.sendMyStateToPeerSync(peer)
			}
		}
	})
}

func (z *ZmqMultiGcounter) Increment(key string) {
	z.Act(z, func() {
		counter := z.getOrCreateCounterSync(key)
		counter.Increment()
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

func (c *ZmqMultiGcounter) Value(key string) int64 {
	var val int64
	phony.Block(c, func() {
		counter := c.getOrCreateCounterSync(key)
		val = counter.Value()
	})
	return val
}

func (c *ZmqMultiGcounter) GetCounter(key string) *PersistentGCounter {
	return c.getOrCreateCounterSync(key)
}

func (c *ZmqMultiGcounter) PersistSync() {
	for _, counter := range c.inner {
		counter.PersistSync()
	}
}

func (c *ZmqMultiGcounter) PersistOneSync(key string) {
	phony.Block(c, func() {
		counter := c.getOrCreateCounterSync(key)
		counter.PersistSync()
	})
}

func (z *ZmqMultiGcounter) getOrCreateCounterSync(key string) *PersistentGCounter {
	if counter, ok := z.inner[key]; ok {
		return counter
	}

	counter := NewPersistentGCounterWithSinkAndObserver(z.identity, z.multiCounterFilenameFor(key), z, z.observer)
	counter.inner.state.Name = key
	// to do: improve construction
	z.inner[key] = counter
	return counter
}

func (z *ZmqMultiGcounter) receiveLoop() {
	log.Printf("started listening to incoming messages at %s", z.bindAddr)
	for {
		select {
		case <-z.ctx.Done():
			log.Printf("stopped listening to incoming messages at %s", z.bindAddr)
			z.started = false
			return
		default:
		}

		msg, err := z.server.Recv()
		if err != nil {
			log.Printf("failed receive: %v", err)
			go z.receiveLater()
			return
		}
		state := GCounterState{}
		err = json.Unmarshal(msg.Bytes(), &state)
		if err != nil {
			log.Printf("failed to deserialize state: %v", err)
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
		log.Printf("client not found for peer %s", peer)
		return
	}

	// send all counters
	for _, counter := range z.inner {
		sendStateToPeerSync(client, counter.GetState())
	}
}

func (z *ZmqMultiGcounter) multiCounterFilenameFor(key string) string {
	return path.Join(z.dirname, key+".gcounter")
}

func nameOrSingleton(name string) string {
	if name != "" {
		return name
	}
	return "singleton"
}
