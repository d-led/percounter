package percounter

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var globalEmergencyPersistence EmergencyPersistence

type EmergencyPersistence struct {
	toPersist sync.Map
	signals   chan os.Signal
	done      chan bool
}

func (s *EmergencyPersistence) Init() {
	if s.signals != nil {
		// already initialized
		return
	}

	// initialize on demand or upon first add
	s.signals = make(chan os.Signal, 1)
	s.done = make(chan bool, 1)
	signal.Notify(s.signals, syscall.SIGINT, syscall.SIGTERM)
}

func (s *EmergencyPersistence) AddForPersistence(p Persistent) {
	s.Init()
	s.toPersist.Store(p, true)
}

func (s *EmergencyPersistence) PersistAndExitOnSignal() {
	if s.signals == nil {
		return
	}
	go func() {
		sig := <-s.signals
		log.Printf("Received signal: %v, persisting", sig)
		s.toPersist.Range(func(p, _ any) bool {
			p.(Persistent).PersistSync()
			log.Println(".")
			return true
		})
		s.done <- true
	}()
	<-s.done
	log.Println("exiting")
	os.Exit(0)
}

func GlobalEmergencyPersistence() *EmergencyPersistence {
	return &globalEmergencyPersistence
}

func init() {
	globalEmergencyPersistence = EmergencyPersistence{
		toPersist: sync.Map{},
	}
}
