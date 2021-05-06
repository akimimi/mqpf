package mqpf

import (
	"encoding/base64"
	"fmt"
	"github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/gogap/logs"
)

type QueueEventHandlerInterface interface {
	// BeforeLaunch function is invoked when framework Launch function starts.
	// qf QueueFramework is the framework
	BeforeLaunch(qf QueueFramework)

	// AfterLaunch function is invoked when framework Launch function starts.
	// qf QueueFramework is the framework
	AfterLaunch(qf QueueFramework)

	// OnWaitingMessage is invoked when queue framework starts to wait for one queue message.
	// User can log queue status or do something besides normal dispose flow.
	OnWaitingMessage(qf QueueFramework)

	// ParseMessageBody decodes the message body and is invoked when message is received.
	// The decoded message will be passed to ConsumeMessage interface as the first parameter.
	ParseMessageBody(resp *ali_mns.MessageReceiveResponse) ([]byte, error)

	// OnParseMessageBodyFailed is invoked if ParseMessageBody return a non-nil error.
	// User can log and deal with the error and response body in this function.
	OnParseMessageBodyFailed(err error, resp *ali_mns.MessageReceiveResponse)

	// ConsumeMessage is the entry for user business logic. The decoded body and response struct are provided.
	ConsumeMessage(body []byte, resp *ali_mns.MessageReceiveResponse) error

	// OnConsumeFailed is invoked if ConsumeMessage return a non-nil error.
	OnConsumeFailed(err error, body []byte, resp *ali_mns.MessageReceiveResponse)

	// BeforeChangeVisibility is invoked before the queue framework changes message visibility.
	BeforeChangeVisibility(q ali_mns.AliMNSQueue, resp *ali_mns.MessageReceiveResponse)

	// AfterChangeVisibility is invoked after the queue framework changes message visibility.
	AfterChangeVisibility(q ali_mns.AliMNSQueue, resp *ali_mns.MessageReceiveResponse,
		vr *ali_mns.MessageVisibilityChangeResponse)

	// OnChangeVisibilityFailed is invoked if the queue framework can't change message visibility.
	OnChangeVisibilityFailed(q ali_mns.AliMNSQueue, resp *ali_mns.MessageReceiveResponse,
		vr *ali_mns.MessageVisibilityChangeResponse)

	// OnError is invoked whenever an error happens.
	OnError(err error, q ali_mns.AliMNSQueue,
		rr *ali_mns.MessageReceiveResponse, vr *ali_mns.MessageVisibilityChangeResponse,
		qf QueueFramework)
}

type DefaultEventHandler struct{}

func (d *DefaultEventHandler) BeforeLaunch(qf QueueFramework) {
	if qf != nil {
		qf.RegisterBreakQueueOsSingal()
		stat := qf.GetStatistic()
		if stat != nil {
			stat.Start()
		}
	}
}

func (d *DefaultEventHandler) AfterLaunch(_ QueueFramework) {
}

func (d *DefaultEventHandler) OnWaitingMessage(qf QueueFramework) {
	stat := qf.GetStatistic()
	if stat != nil {
		stat.Monitor()
	}
}

func (d *DefaultEventHandler) ParseMessageBody(resp *ali_mns.MessageReceiveResponse) ([]byte, error) {
	if resp != nil {
		return base64.StdEncoding.DecodeString(resp.MessageBody)
	} else {
		return nil, nil
	}
}

func (d *DefaultEventHandler) OnParseMessageBodyFailed(_ error, _ *ali_mns.MessageReceiveResponse) {
}

func (d *DefaultEventHandler) ConsumeMessage(_ []byte, _ *ali_mns.MessageReceiveResponse) error {
	return nil
}

func (d *DefaultEventHandler) OnConsumeFailed(_ error, _ []byte, _ *ali_mns.MessageReceiveResponse) {
}

func (d *DefaultEventHandler) BeforeChangeVisibility(_ ali_mns.AliMNSQueue, _ *ali_mns.MessageReceiveResponse) {
}
func (d *DefaultEventHandler) AfterChangeVisibility(_ ali_mns.AliMNSQueue,
	_ *ali_mns.MessageReceiveResponse,
	_ *ali_mns.MessageVisibilityChangeResponse) {
}

func (d *DefaultEventHandler) OnChangeVisibilityFailed(_ ali_mns.AliMNSQueue,
	_ *ali_mns.MessageReceiveResponse,
	_ *ali_mns.MessageVisibilityChangeResponse) {
}

func (d *DefaultEventHandler) OnError(err error, queue ali_mns.AliMNSQueue,
	resp *ali_mns.MessageReceiveResponse,
	vret *ali_mns.MessageVisibilityChangeResponse,
	qf QueueFramework) {
	if resp != nil && resp.DequeueCount >= int64(qf.GetConfig().MaxDequeueCount) {
		if e := queue.DeleteMessage(vret.ReceiptHandle); e == nil {
			logs.Info(fmt.Sprintf("Message Dequeue %d (force deleted): %s",
				resp.DequeueCount, vret.ReceiptHandle))
		} else {
			if qf.GetConfig().Verbose {
				logs.Debug("Force delete message failed!", e)
			}
		}
	} else {
		logs.Error(err)
	}
}
