package cwatsch_test

import (
	cw "github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

type cwMock struct {
	cloudwatchiface.CloudWatchAPI
}

func (mock *cwMock) PutMetricData(input *cw.PutMetricDataInput) (*cw.PutMetricDataOutput, error) {
	return nil, nil
}

var cwAPI = cwMock{}
