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
		m.CollectNextGC = true
		m.CollectStackInuse = true
		m.CollectPauseTotalNs = true
		m.CollectNumGC = true
		m.CollectHeapObjects = true
		m.CollectNumGoroutine = true

		// the metrics will be collected every minute
		m.Launch(ctx, time.Minute)
	}()
	// Output:
}
