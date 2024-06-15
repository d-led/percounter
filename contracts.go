package percounter

type GCounterStateSource interface {
	GetState() GCounterState
}

type GCounterStateSink interface {
	SetState(s GCounterState)
}

type ValueSource interface {
	Value() int64
}

type noOpGcounterState struct{}

func (n *noOpGcounterState) GetState() GCounterState  { return NewGcounterState() }
func (n *noOpGcounterState) SetState(s GCounterState) {}
