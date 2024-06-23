package main

import (
	"log"

	"github.com/d-led/percounter"
)

func main() {
	percounter.GlobalEmergencyPersistence().AddForPersistence(percounter.NewPersistentGCounter("a", "a.gcounter"))
	percounter.GlobalEmergencyPersistence().AddForPersistence(percounter.NewPersistentGCounter("b", "b.gcounter"))
	log.Println("added 2 counters to persist in case of signals. Press Ctrl+C to persist and exit")
	percounter.GlobalEmergencyPersistence().PersistAndExitOnSignal()
}
