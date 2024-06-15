package percounter

type GCounterStateSource interface {
	GetState() GCounterState
}

type GCounterStateSink interface {
	SetState(s GCounterState)
}

type noOpGcounterState struct{}

func (n *noOpGcounterState) GetState() GCounterState  { return NewGcounterState() }
func (n *noOpGcounterState) SetState(s GCounterState) {}
