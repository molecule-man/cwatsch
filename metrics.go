package cwatsch

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	cw "github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

const maxBatchSize = 20

type Batch struct {
	sync.Mutex
	cwAPI    cloudwatchiface.CloudWatchAPI
	metricQs map[string]*queue

	autoFlushCtx          context.Context
	autoFlushTimeInterval time.Duration
	autoFlushOnError      func(error)
}

func New(cwAPI cloudwatchiface.CloudWatchAPI, opts ...Option) *Batch {
	b := &Batch{
		cwAPI:    cwAPI,
		metricQs: map[string]*queue{},
	}

	for _, o := range opts {
		o(b)
	}

	if b.autoFlushTimeInterval != 0 {
		go func() {
			for {
				select {
				case <-time.After(b.autoFlushTimeInterval):
					err := b.Flush()
					if err != nil && b.autoFlushOnError != nil {
						b.autoFlushOnError(err)
					}
				case <-b.autoFlushCtx.Done():
					break
				}
			}
		}()
	}

	return b
}

func (b *Batch) PutMetricData(input *cw.PutMetricDataInput) (*cw.PutMetricDataOutput, error) {
	b.AddInputs(input)
	return &cw.PutMetricDataOutput{}, nil
}

func (b *Batch) Add(namespace string, data ...*cw.MetricDatum) *Batch {
	b.add(&cw.PutMetricDataInput{
		Namespace:  aws.String(namespace),
		MetricData: data,
	})

	return b
}

func (b *Batch) AddInputs(inputs ...*cw.PutMetricDataInput) *Batch {
	for _, i := range inputs {
		b.add(i)
	}

	return b
}

func (b *Batch) add(input *cw.PutMetricDataInput) {
	b.Lock()
	defer b.Unlock()

	ns := aws.StringValue(input.Namespace)

	q, ok := b.metricQs[ns]
	if !ok {
		q = &queue{
			nodes: make([]*cw.MetricDatum, maxBatchSize),
			size:  maxBatchSize,
		}
		b.metricQs[ns] = q
	}

	for _, datum := range input.MetricData {
		q.push(datum)
	}
}

func (b *Batch) FlushIfFilled() error {
	flush := flush{cwAPI: b.cwAPI, errs: make(chan error)}

	b.Lock()
	for ns, q := range b.metricQs {
		for q.count >= maxBatchSize {
			flush.do(ns, q.top(maxBatchSize))
		}
	}
	b.Unlock()

	return flush.error()
}

func (b *Batch) Flush() error {
	b.Lock()
	metricQs := b.metricQs
	b.metricQs = map[string]*queue{}
	b.Unlock()

	flush := flush{cwAPI: b.cwAPI, errs: make(chan error)}

	for ns, q := range metricQs {
		for q.count > 0 {
			flush.do(ns, q.top(maxBatchSize))
		}
	}

	return flush.error()
}

type Option func(*Batch)

// WithAutoFlush configures Batch to flush metrics periodically after specified
// period of time. It accepts context which provides means to cancel auto-flush.
func WithAutoFlush(ctx context.Context, timeInterval time.Duration, onError func(err error)) Option {
	return func(b *Batch) {
		b.autoFlushCtx = ctx
		b.autoFlushTimeInterval = timeInterval
		b.autoFlushOnError = onError
	}
}

type queue struct {
	nodes []*cw.MetricDatum
	size  int
	head  int
	tail  int
	count int
}

func (q *queue) push(n *cw.MetricDatum) {
	if q.head == q.tail && q.count > 0 {
		nodes := make([]*cw.MetricDatum, len(q.nodes)+q.size)
		copy(nodes, q.nodes[q.head:])
		copy(nodes[len(q.nodes)-q.head:], q.nodes[:q.head])
		q.head = 0
		q.tail = len(q.nodes)
		q.nodes = nodes
	}

	q.nodes[q.tail] = n
	q.tail = (q.tail + 1) % len(q.nodes)
	q.count++
}

func (q *queue) pop() *cw.MetricDatum {
	if q.count == 0 {
		return nil
	}

	node := q.nodes[q.head]
	q.head = (q.head + 1) % len(q.nodes)
	q.count--

	return node
}

func (q *queue) top(n int) []*cw.MetricDatum {
	if q.count < n {
		n = q.count
	}

	result := make([]*cw.MetricDatum, 0, n)

	for i := 0; i < n; i++ {
		result = append(result, q.pop())
	}

	return result
}

type flush struct {
	cwAPI     cloudwatchiface.CloudWatchAPI
	errs      chan error
	execCount int
}

func (f *flush) do(ns string, batch []*cw.MetricDatum) {
	go func(ns string, batch []*cw.MetricDatum) {
		_, err := f.cwAPI.PutMetricData(&cw.PutMetricDataInput{
			Namespace:  aws.String(ns),
			MetricData: batch,
		})
		f.errs <- err
	}(ns, batch)
	f.execCount++
}

func (f *flush) error() error {
	var lastErr error

	for i := 0; i < f.execCount; i++ {
		if err := <-f.errs; err != nil {
			lastErr = err
		}
	}

	return lastErr
}
