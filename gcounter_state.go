package percounter

import "maps"

type GCounterState struct {
	Name  string           `json:"name"`
	Peers map[string]int64 `json:"peers"`
}

type NetworkedGCounterState struct {
	Type       string                 `json:"type"`
	SourcePeer string                 `json:"source_peer"`
	Name       string                 `json:"name"`
	Peers      map[string]int64       `json:"peers"`
	Metadata   map[string]interface{} `json:"metadata"`
}

func NewGcounterState() GCounterState {
	return NewNamedGcounterState("singleton")
}

func NewNamedGcounterState(name string) GCounterState {
	return GCounterState{
		Name:  name,
		Peers: make(map[string]int64),
	}
}

func (s GCounterState) Copy() GCounterState {
	return GCounterState{
		Name:  s.Name,
		Peers: maps.Clone(s.Peers),
	}
}
