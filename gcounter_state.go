package percounter

type GCounterState struct {
	Peers map[string]int64 `json:"peers"`
}

func NewGcounterState() GCounterState {
	return GCounterState{
		Peers: make(map[string]int64),
	}
}
