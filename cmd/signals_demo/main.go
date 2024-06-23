package main

import (
	"log"

	"github.com/d-led/percounter"
)

func main() {
	// this forces `PersistAndExitOnSignal` to wait even in the absence of added persistent counters
	percounter.GlobalEmergencyPersistence().Init()
	percounter.GlobalEmergencyPersistence().AddForPersistence(percounter.NewPersistentGCounter("a", "a.gcounter"))
	percounter.GlobalEmergencyPersistence().AddForPersistence(percounter.NewPersistentGCounter("b", "b.gcounter"))
	mc := percounter.NewZmqMultiGcounter("me", ".", ":5000")
	mc.ShouldPersistOnSignal()
	mc.Start()
	defer mc.Stop()
	mc.Increment("c")
	mc.Increment("d")
	log.Println("added 2 counters to persist in case of signals. Press Ctrl+C to persist and exit")
	percounter.GlobalEmergencyPersistence().PersistAndExitOnSignal()
}
