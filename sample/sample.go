package main

import (
	"github.com/akimimi/mqpf"
	cl "github.com/akimimi/config-loader"
	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/gogap/logs"
)

func main() {
	queueConfig := cl.QueueConfig{}
	queueConfig.LoadByFile("sample/sample_queue_config.yaml")

	client := ali_mns.NewAliMNSClient(
		queueConfig.Url, queueConfig.AccessKeyId, queueConfig.AccessKeySecret)
	queueManager := ali_mns.NewMNSQueueManager(client)

	// create queue to make sure queue exists
	err := queueManager.CreateQueue(queueConfig.QueueName, int32(queueConfig.DelaySeconds),
		int32(queueConfig.MaxMessageSize), int32(queueConfig.MessageRetentionPeriod),
		int32(queueConfig.VisibilityTimeout), int32(queueConfig.PollingWaitSeconds), 2)

	if err != nil && !ali_mns.ERR_MNS_QUEUE_ALREADY_EXIST_AND_HAVE_SAME_ATTR.IsEqual(err) {
		if queueConfig.Verbose {
			logs.Debug("Queue", queueConfig.QueueName, "has been created!")
			logs.Debug(err)
		}
	}
	queue := ali_mns.NewMNSQueue(queueConfig.QueueName, client)

	qf := mqpf.NewQueueFramework(queue, queueConfig, &EventHandler{})
	qf.Launch()
}

type EventHandler struct {
	mqpf.DefaultEventHandler
}

func (e *EventHandler) BeforeLaunch(qf mqpf.QueueFramework) {
	logs.Info("EventHandler before launch")
	e.DefaultEventHandler.BeforeLaunch(qf)
}

func (e *EventHandler) AfterLaunch(_ mqpf.QueueFramework) {
	logs.Info("EventHandler after launch")
}

func (e *EventHandler) ConsumeMessage(body []byte, _ *ali_mns.MessageReceiveResponse) error {
	logs.Info("Consume message: ", string(body))
	return nil
}

func (e *EventHandler) OnError(err error, q ali_mns.AliMNSQueue,
	rr *ali_mns.MessageReceiveResponse, vr *ali_mns.MessageVisibilityChangeResponse, qf mqpf.QueueFramework) {

	if ali_mns.ERR_MNS_MESSAGE_NOT_EXIST.IsEqual(err) || q == nil || rr == nil || vr == nil {
		return
	}
	e.DefaultEventHandler.OnError(err, q, rr, vr, qf)
}
