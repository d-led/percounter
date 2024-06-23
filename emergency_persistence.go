package percounter

import (
	"log"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"

	"github.com/Arceliar/phony"
)

var globalEmergencyPersistence EmergencyPersistence

type EmergencyPersistence struct {
	phony.Inbox
	wg        sync.WaitGroup
	toPersist []Persistent
	signals   chan os.Signal
}

func (s *EmergencyPersistence) AddForPersistence(p Persistent) {
	s.Act(s, func() {
		if len(s.toPersist) == 0 {
			s.signals = make(chan os.Signal, 1)
			signal.Notify(s.signals, syscall.SIGINT, syscall.SIGTERM)
		}

		s.toPersist = append(s.toPersist, p)
		s.wg.Add(1)
	})
}

func (s *EmergencyPersistence) PersistAndExitOnSignal() {
	phony.Block(s, func() {
		toPersist := slices.Clone(s.toPersist)
		go func() {
			sig := <-s.signals
			log.Printf("Received signal: %v, persisting", sig)
			for _, p := range toPersist {
				p.PersistSync()
				log.Println(".")
				s.wg.Done()
			}
		}()
	})
	s.wg.Wait()
	os.Exit(0)
}

func GlobalEmergencyPersistence() *EmergencyPersistence {
	return &globalEmergencyPersistence
}

func init() {
	globalEmergencyPersistence = EmergencyPersistence{
		toPersist: []Persistent{},
	}
}
