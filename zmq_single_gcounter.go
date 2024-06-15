package percounter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Arceliar/phony"
	"github.com/go-zeromq/zmq4"
)

type ZmqSingleGcounter struct {
	phony.Inbox
	inner    *PersistentGCounter
	bindAddr string
	server   zmq4.Socket
	peers    map[string]zmq4.Socket
	started  bool
	ctx      context.Context
	stop     context.CancelFunc
}

func NewZmqSingleGcounter(identity, filename, bindAddr string) *ZmqSingleGcounter {
	ctx, cancel := context.WithCancel(context.Background())
	res := &ZmqSingleGcounter{
		bindAddr: bindAddr,
		server:   zmq4.NewPull(ctx),
		peers:    make(map[string]zmq4.Socket),
		ctx:      ctx,
		stop:     cancel,
	}
	res.inner = NewPersistentGCounterWithSink(identity, filename, res)
	return res
}

func (z *ZmqSingleGcounter) Start() error {
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

func (z *ZmqSingleGcounter) Stop() {
	z.stop()
	phony.Block(z, func() {
		z.server.Close()
	})
}

func (z *ZmqSingleGcounter) UpdatePeers(peers []string) {
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

func (z *ZmqSingleGcounter) receiveLoop() {
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
		z.MergeWith(NewGCounterFromState("temporary-counter", state))
	}
}

func (z *ZmqSingleGcounter) receiveLater() {
	time.Sleep(100 * time.Millisecond)
	z.receiveLoop()
}

func (z *ZmqSingleGcounter) propagateStateSync(s GCounterState) {
	for _, client := range z.peers {
		sendStateToPeerSync(client, s)
	}
}

func (z *ZmqSingleGcounter) sendMyStateToPeerSync(peer string) {
	client, ok := z.peers[peer]
	if !ok {
		log.Printf("client not found for peer %s", peer)
		return
	}

	sendStateToPeerSync(client, z.inner.GetState())
}

func sendStateToPeerSync(c zmq4.Socket, s GCounterState) {
	msg, err := json.Marshal(s)
	if err != nil {
		log.Printf("error serializing state: %v", err)
		return
	}

	err = c.Send(zmq4.NewMsg(msg))
	if err != nil {
		log.Printf("error sending state state: %v", err)
		return
	}
}

func setOf(s []string) map[string]bool {
	var res = make(map[string]bool)
	for _, e := range s {
		res[e] = true
	}
	return res
}
