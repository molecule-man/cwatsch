package cwatsch

import (
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	cw "github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

const maxBatchSize = 20

type Batch struct {
	sync.Mutex
	cwAPI   cloudwatchiface.CloudWatchAPI
	metrics map[string]batchedMetrics
}

func New(cwAPI cloudwatchiface.CloudWatchAPI) *Batch {
	return &Batch{
		cwAPI:   cwAPI,
		metrics: map[string]batchedMetrics{},
	}
}

func (b *Batch) PutMetricData(input *cw.PutMetricDataInput) (*cw.PutMetricDataOutput, error) {
	b.AddMetric(input)
	return &cw.PutMetricDataOutput{}, nil
}

func (b *Batch) AddMetric(input *cw.PutMetricDataInput) {
	b.Lock()
	defer b.Unlock()

	ns := aws.StringValue(input.Namespace)

	batches, ok := b.metrics[ns]
	if !ok {
		batches = batchedMetrics{}
	}

	batches.add(input)
	b.metrics[ns] = batches
}

func (b *Batch) Flush() error {
	b.Lock()
	metrics := b.metrics
	b.metrics = map[string]batchedMetrics{}
	b.Unlock()

	errs := make(chan error)
	errsNum := 0

	for ns, batchesInNamespace := range metrics {
		for _, batch := range batchesInNamespace {
			input := &cw.PutMetricDataInput{
				Namespace:  aws.String(ns),
				MetricData: batch,
			}
			go func(input *cw.PutMetricDataInput) {
				_, err := b.cwAPI.PutMetricData(input)
				errs <- err
			}(input)
			errsNum++
		}
	}

	var lastErr error

	for i := 0; i < errsNum; i++ {
		if err := <-errs; err != nil {
			lastErr = err
		}
	}

	return lastErr
}

type batchedMetrics [][]*cw.MetricDatum

func (mPtr *batchedMetrics) add(input *cw.PutMetricDataInput) {
	m := *mPtr

	if len(m) == 0 {
		m = append(m, make([]*cw.MetricDatum, 0, maxBatchSize))
	}

	if len(m[len(m)-1]) >= maxBatchSize {
		m = append(m, make([]*cw.MetricDatum, 0, maxBatchSize))
	}

	m[len(m)-1] = append(m[len(m)-1], input.MetricData...)

	*mPtr = m
}
