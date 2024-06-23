package percounter

import (
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func waitForGcounterValueOf(t *testing.T, expectedValue int64, c ValueSource) {
	for w := 0; w < 15; w++ {
		if expectedValue == c.Value() {
			// all ok
			return
		}
		log.Printf("waiting for the counter to arrive at the expected value of %d ...", expectedValue)
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(t, expectedValue, c.Value())
}

func newTempFilename(t *testing.T) string {
	f, err := os.CreateTemp(".", "*.gcounter")
	if err != nil {
		panic(err)
	}
	fn := f.Name()
	_ = f.Close()
	t.Cleanup(func() { _ = os.Remove(fn) })
	return fn
}

type testGCounterStateSink struct {
	lastState GCounterState
}

func (sink *testGCounterStateSink) SetState(s GCounterState) {
	sink.lastState = s
}

func randomPort() string {
	return fmt.Sprint(5000 + rand.Int32N(2000))
}
