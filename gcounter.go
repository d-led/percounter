package percounter

type GCounterState struct {
	peers map[string]int64
}

type GCounter struct {
	identity string
	state    GCounterState
}

func NewGCounter(identity string) *GCounter {
	return &GCounter{
		identity: identity,
		state: GCounterState{
			peers: make(map[string]int64),
		},
	}
}

func (c *GCounter) Increment() {
	val := c.state.peers[c.identity]
	val++
	c.state.peers[c.identity] = val
}

func (c *GCounter) Value() int64 {
	if val, ok := c.state.peers[c.identity]; ok {
		return val
	}
	return 0
}
