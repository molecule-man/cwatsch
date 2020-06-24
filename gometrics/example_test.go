package gometrics_test

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/molecule-man/cwatsch/gometrics"
)

func ExampleNew() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		m := gometrics.New(session.Must(session.NewSession()))
		m.Namespace = "MyApp"
		m.CollectHeapAlloc = true
		m.CollectNextGC = true       // target heap size of the next GC cycle
		m.CollectStackInuse = true   // bytes in stack spans.
		m.CollectPauseTotalNs = true // cumulative nanoseconds in GC stop-the-world pauses since the program started.
		m.CollectNumGC = true        // number of completed GC cycles.
		m.CollectHeapObjects = true  // number of allocated heap objects.
		m.CollectNumGoroutine = true

		// the metrics will be collected every minute
		m.Launch(ctx, time.Minute)
	}()
	// Output:
}
