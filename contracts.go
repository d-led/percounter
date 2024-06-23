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

type Persistent interface {
	PersistSync()
}

type ValueSource interface {
	Value() int64
}

type CountEvent struct {
	Name  string
	Count int64
}

type ClusterObserver interface {
	AfterMessageSent(peer string, msg interface{})
	AfterMessageReceived(peer string, msg interface{})
}

type CounterObserver interface {
	OnNewCount(ev CountEvent)
}

type Incrementable interface {
	Increment()
}

type noOpGcounterState struct{}

func (n *noOpGcounterState) GetState() GCounterState  { return NewGcounterState() }
func (n *noOpGcounterState) SetState(s GCounterState) {}

type noOpCounterObserver struct{}

func (n *noOpCounterObserver) OnNewCount(CountEvent) {}
