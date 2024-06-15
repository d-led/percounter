package percounter

import "math"

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
	val := c.valueOf(c.identity)
	val++
	c.setValueOf(c.identity, val)
}

func (c *GCounter) Value() int64 {
	var res int64
	for _, value := range c.state.Peers {
		res += value
	}
	return res
}

func (c *GCounter) MergeWith(other *GCounter) {
	for peer, value := range other.state.Peers {
		myValue := c.valueOf(peer)
		newValue := int64(math.Max(float64(value), float64(myValue)))
		c.setValueOf(peer, newValue)
	}
}

func (c *GCounter) valueOf(peer string) int64 {
	if val, ok := c.state.Peers[peer]; ok {
		return val
	}
	return 0
}

func (c *GCounter) setValueOf(peer string, val int64) {
	c.state.Peers[peer] = val
}
