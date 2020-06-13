package cwatsch

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	cw "github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cwMock struct {
	cloudwatchiface.CloudWatchAPI
	sync.Mutex
	capturedPayloads []*cw.PutMetricDataInput
}

func (mock *cwMock) PutMetricData(input *cw.PutMetricDataInput) (*cw.PutMetricDataOutput, error) {
	mock.Lock()
	defer mock.Unlock()

	if mock.capturedPayloads == nil {
		mock.capturedPayloads = []*cw.PutMetricDataInput{}
	}

	mock.capturedPayloads = append(mock.capturedPayloads, input)

	return nil, nil
}

func sortByNS(payloads []*cw.PutMetricDataInput) []*cw.PutMetricDataInput {
	sort.Slice(payloads, func(i, j int) bool {
		return aws.StringValue(payloads[i].Namespace) < aws.StringValue(payloads[j].Namespace)
	})

	return payloads
}

func sortBySize(payloads []*cw.PutMetricDataInput) []*cw.PutMetricDataInput {
	sort.Slice(payloads, func(i, j int) bool {
		return len(payloads[i].MetricData) > len(payloads[j].MetricData)
	})

	return payloads
}

func TestGroupingByNamespace(t *testing.T) {
	cwAPI := cwMock{}
	batch := New(&cwAPI)

	batch.AddInputs(&cw.PutMetricDataInput{
		Namespace: aws.String("namespace1"),
		MetricData: []*cw.MetricDatum{
			{MetricName: aws.String("namespace1 metric1")},
		},
	})
	batch.AddInputs(&cw.PutMetricDataInput{
		Namespace: aws.String("namespace1"),
		MetricData: []*cw.MetricDatum{
			{MetricName: aws.String("namespace1 metric2")},
			{MetricName: aws.String("namespace1 metric3")},
		},
	})
	batch.AddInputs(&cw.PutMetricDataInput{
		Namespace: aws.String("namespace2"),
		MetricData: []*cw.MetricDatum{
			{MetricName: aws.String("namespace2 metric1")},
		},
	})

	require.NoError(t, batch.Flush())

	assert.Equal(t, []*cw.PutMetricDataInput{{
		Namespace: aws.String("namespace1"),
		MetricData: []*cw.MetricDatum{
			{MetricName: aws.String("namespace1 metric1")},
			{MetricName: aws.String("namespace1 metric2")},
			{MetricName: aws.String("namespace1 metric3")},
		},
	}, {
		Namespace: aws.String("namespace2"),
		MetricData: []*cw.MetricDatum{{
			MetricName: aws.String("namespace2 metric1"),
		}},
	}}, sortByNS(cwAPI.capturedPayloads))
}

func TestBatchSizeIsLimitedBy20Items(t *testing.T) {
	cwAPI := cwMock{}
	batch := New(&cwAPI)

	for i := 0; i < 82; i++ {
		batch.Add("", &cw.MetricDatum{MetricName: aws.String(fmt.Sprintf("metric%d", i))})
	}

	require.NoError(t, batch.Flush())

	sortBySize(cwAPI.capturedPayloads)

	require.Len(t, cwAPI.capturedPayloads, 5)
	assert.Len(t, cwAPI.capturedPayloads[0].MetricData, 20)
	assert.Len(t, cwAPI.capturedPayloads[1].MetricData, 20)
	assert.Len(t, cwAPI.capturedPayloads[2].MetricData, 20)
	assert.Len(t, cwAPI.capturedPayloads[3].MetricData, 20)
	assert.Equal(t, cw.PutMetricDataInput{
		Namespace: aws.String(""),
		MetricData: []*cw.MetricDatum{
			{MetricName: aws.String("metric80")},
			{MetricName: aws.String("metric81")},
		},
	}, *cwAPI.capturedPayloads[4])
}

func TestFlushIfFilled(t *testing.T) {
	cwAPI := cwMock{}
	batch := New(&cwAPI)

	for i := 0; i < 19; i++ {
		err := batch.Add("", &cw.MetricDatum{MetricName: aws.String(fmt.Sprintf("metric%d", i+1))}).
			FlushIfFilled()
		require.NoError(t, err)

		assert.Len(t, cwAPI.capturedPayloads, 0, "iteration %d", i)
	}

	err := batch.Add("", &cw.MetricDatum{MetricName: aws.String("metric20")}).FlushIfFilled()
	require.NoError(t, err)

	assert.Len(t, cwAPI.capturedPayloads, 1)
	assert.Len(t, cwAPI.capturedPayloads[0].MetricData, 20)
}

func TestAutoFlush(t *testing.T) {
	cwAPI := cwMock{}
	batch := New(&cwAPI, WithAutoFlush(context.TODO(), 5*time.Millisecond, func(err error) {
		assert.NoError(t, err)
	}))

	for i := 0; i < 10; i++ {
		err := batch.Add("", &cw.MetricDatum{MetricName: aws.String(fmt.Sprintf("metric%d", i+1))}).
			FlushIfFilled()
		require.NoError(t, err)

		assert.Len(t, cwAPI.capturedPayloads, 0, "iteration %d", i)
	}

	assert.Len(t, cwAPI.capturedPayloads, 0)

	time.Sleep(10 * time.Millisecond)

	assert.Len(t, cwAPI.capturedPayloads, 1)
	assert.Len(t, cwAPI.capturedPayloads[0].MetricData, 10)
}
