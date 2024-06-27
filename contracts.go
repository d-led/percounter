package percounter

const GCounterNetworkMessage = "g-counter.network.message"
const PeerOhaiNetworkMessage = "peer.ohai.network.message"

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
	AfterMessageSent(peer string, msg []byte)
	AfterMessageReceived(peer string, msg []byte)
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
