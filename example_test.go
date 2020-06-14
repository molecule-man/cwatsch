package cwatsch_test

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/molecule-man/cwatsch"
)

func ExampleBatch_LaunchAutoFlush() {
	batch := cwatsch.New(&cwAPI)
	defer batch.Flush()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// the metrics will be auto-flushed every 30s
	batch.LaunchAutoFlush(ctx, 30*time.Second, func(err error) {
		log.Println(err)
	})

	batch.Add("myApp", &cloudwatch.MetricDatum{
		MetricName: aws.String("number_of_calls"),
		Value:      aws.Float64(1),
		Timestamp:  aws.Time(time.Now()),
	})
	// Output:
}
