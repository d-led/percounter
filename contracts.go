package percounter

type GCounterStateSource interface {
	GetState() GCounterState
}

type GCounterStateSink interface {
	SetState(s GCounterState)
}

type QueryableCounter interface {
	Incrementable
	ValueSource
}

type ValueSource interface {
	Value() int64
}

type CounterObserver interface {
	OnNewCount(count int64)
}

type Incrementable interface {
	Increment()
}

type noOpGcounterState struct{}

func (n *noOpGcounterState) GetState() GCounterState  { return NewGcounterState() }
func (n *noOpGcounterState) SetState(s GCounterState) {}

type noOpCounterObserver struct{}

func (n *noOpCounterObserver) OnNewCount(int64) {}
