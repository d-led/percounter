package percounter

type GCounterState struct {
	Name  string           `json:"name"`
	Peers map[string]int64 `json:"peers"`
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
