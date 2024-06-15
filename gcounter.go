package percounter

type GCounter struct {
	identity string
	state    GCounterState
}

func NewGCounterFromState(identity string, state GCounterState) *GCounter {
	return &GCounter{
		identity: identity,
		state:    state,
	}
}

func NewGCounter(identity string) *GCounter {
	return NewGCounterFromState(identity, NewGcounterState())
}

func (c *GCounter) Increment() {
	val := c.state.Peers[c.identity]
	val++
	c.state.Peers[c.identity] = val
}

func (c *GCounter) Value() int64 {
	if val, ok := c.state.Peers[c.identity]; ok {
		return val
	}
	return 0
}
