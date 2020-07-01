package gometrics

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/molecule-man/cwatsch"
)

// New creates collector of go metrics. Upon creation enable required metrics by
// toggling appropriate GoMetrics.Collect* fields.
func New(cfg client.ConfigProvider) *GoMetrics {
	goMetrics := &GoMetrics{
		Namespace: "gometrics",
		batch:     cwatsch.New(cloudwatch.New(cfg)),
	}
	goMetrics.determineECSDimenstions()
	goMetrics.determineEC2Dimenstions(cfg)

	return goMetrics
}

type GoMetrics struct {
	Dimensions []*cloudwatch.Dimension
	Namespace  string
	OnError    func(error)

	CollectTotalAlloc    bool
	CollectSys           bool
	CollectLookups       bool
	CollectMallocs       bool
	CollectFrees         bool
	CollectHeapAlloc     bool
	CollectHeapSys       bool
	CollectHeapIdle      bool
	CollectHeapInuse     bool
	CollectHeapReleased  bool
	CollectHeapObjects   bool
	CollectStackInuse    bool
	CollectStackSys      bool
	CollectMSpanInuse    bool
	CollectMSpanSys      bool
	CollectMCacheInuse   bool
	CollectMCacheSys     bool
	CollectBuckHashSys   bool
	CollectGCSys         bool
	CollectNextGC        bool
	CollectLastGC        bool
	CollectPauseTotalNs  bool
	CollectNumGC         bool
	CollectNumForcedGC   bool
	CollectGCCPUFraction bool
	CollectNumGoroutine  bool

	batch *cwatsch.Batch
}

// Launch starts metric collection which is executed periodically in intervals
// specified by the the second argument.
func (m *GoMetrics) Launch(ctx context.Context, interval time.Duration) {
	var stats runtime.MemStats

	cwatsch.NewTicker(ctx, interval, func() {
		runtime.ReadMemStats(&stats)

		m.add(m.CollectTotalAlloc, "TotalAlloc", float64(stats.TotalAlloc), cloudwatch.StandardUnitBytes)
		m.add(m.CollectSys, "Sys", float64(stats.Sys), cloudwatch.StandardUnitBytes)
		m.add(m.CollectLookups, "Lookups", float64(stats.Lookups), cloudwatch.StandardUnitCount)
		m.add(m.CollectMallocs, "Mallocs", float64(stats.Mallocs), cloudwatch.StandardUnitCount)
		m.add(m.CollectFrees, "Frees", float64(stats.Frees), cloudwatch.StandardUnitCount)
		m.add(m.CollectHeapAlloc, "HeapAlloc", float64(stats.HeapAlloc), cloudwatch.StandardUnitBytes)
		m.add(m.CollectHeapSys, "HeapSys", float64(stats.HeapSys), cloudwatch.StandardUnitBytes)
		m.add(m.CollectHeapIdle, "HeapIdle", float64(stats.HeapIdle), cloudwatch.StandardUnitBytes)
		m.add(m.CollectHeapInuse, "HeapInuse", float64(stats.HeapInuse), cloudwatch.StandardUnitBytes)
		m.add(m.CollectHeapReleased, "HeapReleased", float64(stats.HeapReleased), cloudwatch.StandardUnitBytes)
		m.add(m.CollectHeapObjects, "HeapObjects", float64(stats.HeapObjects), cloudwatch.StandardUnitCount)
		m.add(m.CollectStackInuse, "StackInuse", float64(stats.StackInuse), cloudwatch.StandardUnitBytes)
		m.add(m.CollectStackSys, "StackSys", float64(stats.StackSys), cloudwatch.StandardUnitBytes)
		m.add(m.CollectMSpanInuse, "MSpanInuse", float64(stats.MSpanInuse), cloudwatch.StandardUnitBytes)
		m.add(m.CollectMSpanSys, "MSpanSys", float64(stats.MSpanSys), cloudwatch.StandardUnitBytes)
		m.add(m.CollectMCacheInuse, "MCacheInuse", float64(stats.MCacheInuse), cloudwatch.StandardUnitBytes)
		m.add(m.CollectMCacheSys, "MCacheSys", float64(stats.MCacheSys), cloudwatch.StandardUnitBytes)
		m.add(m.CollectBuckHashSys, "BuckHashSys", float64(stats.BuckHashSys), cloudwatch.StandardUnitBytes)
		m.add(m.CollectGCSys, "GCSys", float64(stats.GCSys), cloudwatch.StandardUnitBytes)
		m.add(m.CollectNextGC, "NextGC", float64(stats.NextGC), cloudwatch.StandardUnitBytes)
		m.add(m.CollectLastGC, "LastGC", float64(stats.LastGC)/1000, cloudwatch.StandardUnitMicroseconds)
		m.add(m.CollectPauseTotalNs, "PauseTotalNs", float64(stats.PauseTotalNs)/1000, cloudwatch.StandardUnitMicroseconds)
		m.add(m.CollectNumGC, "NumGC", float64(stats.NumGC), cloudwatch.StandardUnitCount)
		m.add(m.CollectNumForcedGC, "NumForcedGC", float64(stats.NumForcedGC), cloudwatch.StandardUnitCount)
		m.add(m.CollectGCCPUFraction, "GCCPUFraction", 100.0*stats.GCCPUFraction, cloudwatch.StandardUnitPercent)
		m.add(m.CollectNumGoroutine, "NumGoroutine", float64(runtime.NumGoroutine()), cloudwatch.StandardUnitCount)

		err := m.batch.FlushCompleteBatchesCtx(ctx)
		if err != nil && m.OnError != nil {
			m.OnError(err)
		}
	})
}
func (m *GoMetrics) add(enabled bool, name string, val float64, unit string) {
	if !enabled {
		return
	}

	now := time.Now()

	m.batch.Add(m.Namespace, &cloudwatch.MetricDatum{
		Dimensions: m.Dimensions,
		MetricName: aws.String(name),
		Value:      aws.Float64(val),
		Unit:       aws.String(unit),
		Timestamp:  &now,
	})
}

func (m *GoMetrics) determineECSDimenstions() {
	ecsMetaURI := os.Getenv("ECS_CONTAINER_METADATA_URI")
	if ecsMetaURI == "" {
		return
	}

	hclient := http.Client{
		Timeout: 2 * time.Second,
	}

	r, err := hclient.Get(ecsMetaURI)
	if err != nil {
		return
	}

	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		return
	}

	payload := struct{ DockerID string }{}

	err = json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		return
	}

	m.Dimensions = append(m.Dimensions, &cloudwatch.Dimension{
		Name:  aws.String("ContainerID"),
		Value: aws.String(payload.DockerID),
	})
}

func (m *GoMetrics) determineEC2Dimenstions(sess client.ConfigProvider) {
	client := ec2metadata.New(sess)
	if !client.Available() {
		return
	}

	metadata, err := client.GetInstanceIdentityDocument()
	if err != nil {
		return
	}

	m.Dimensions = append(m.Dimensions,
		&cloudwatch.Dimension{Name: aws.String("InstanceID"), Value: aws.String(metadata.InstanceID)},
		&cloudwatch.Dimension{Name: aws.String("AZ"), Value: aws.String(metadata.AvailabilityZone)},
	)
}
