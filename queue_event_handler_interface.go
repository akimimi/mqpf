package mqpf

import (
	"encoding/base64"
	"fmt"
	"github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/gogap/logs"
)

// QueueEventHandlerInterface defines the interfaces for queue event handler.
// User should implement all the API functions, or derive from DefaultEventHandler struct.
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

	// OnWaitingProcessing is invoked whenever too many messages are processing and the queue will
	// wait for a few seconds. If the queue need to wait for over config.OverloadBreakSeconds,
	// the queue will stop itself.
	OnWaitingProcessing(qf QueueFramework)

	// OnRecoverProcessing is invoked when the queue is recovered from waiting for processing.
	OnRecoverProcessing(qf QueueFramework)

	// OnError is invoked whenever an error happens.
	OnError(err error, q ali_mns.AliMNSQueue,
		rr *ali_mns.MessageReceiveResponse, vr *ali_mns.MessageVisibilityChangeResponse,
		qf QueueFramework)
}

type DefaultEventHandler struct{}

// BeforeLaunch registers system INT, TERM, STOP signals to stop the queue, and HUP signal to update the performance log.
func (d *DefaultEventHandler) BeforeLaunch(qf QueueFramework) {
	if qf != nil {
		qf.RegisterBreakQueueOsSingal()
		stat := qf.GetStatistic()
		if stat != nil {
			stat.Start()
		}
	}
}

// AfterLaunch does nothing by default.
func (d *DefaultEventHandler) AfterLaunch(_ QueueFramework) {
}

// OnWaitingMessage only updates statistic logs by default.
func (d *DefaultEventHandler) OnWaitingMessage(qf QueueFramework) {
	stat := qf.GetStatistic()
	if stat != nil {
		stat.Monitor()
	}
}

// ParseMessageBody decodes response string in base64 by default.
func (d *DefaultEventHandler) ParseMessageBody(resp *ali_mns.MessageReceiveResponse) ([]byte, error) {
	if resp != nil {
		return base64.StdEncoding.DecodeString(resp.MessageBody)
	} else {
		return nil, nil
	}
}

// OnParseMessageBodyFailed does nothing by default.
func (d *DefaultEventHandler) OnParseMessageBodyFailed(_ error, _ *ali_mns.MessageReceiveResponse) {
}

// ConsumeMessage does nothing by default.
func (d *DefaultEventHandler) ConsumeMessage(_ []byte, _ *ali_mns.MessageReceiveResponse) error {
	return nil
}

// OnConsumeFailed does nothing by default.
func (d *DefaultEventHandler) OnConsumeFailed(_ error, _ []byte, _ *ali_mns.MessageReceiveResponse) {
}

// BeforeChangeVisibility does nothing by default.
func (d *DefaultEventHandler) BeforeChangeVisibility(_ ali_mns.AliMNSQueue, _ *ali_mns.MessageReceiveResponse) {
}

// AfterChangeVisibility does nothing by default.
func (d *DefaultEventHandler) AfterChangeVisibility(_ ali_mns.AliMNSQueue,
	_ *ali_mns.MessageReceiveResponse,
	_ *ali_mns.MessageVisibilityChangeResponse) {
}

// OnChangeVisibilityFailed does nothing by default.
func (d *DefaultEventHandler) OnChangeVisibilityFailed(_ ali_mns.AliMNSQueue,
	_ *ali_mns.MessageReceiveResponse,
	_ *ali_mns.MessageVisibilityChangeResponse) {
}

// OnError logs the error in statistic and logger.
// The message will be deleted if dequeue count is over MaxDequeueCount config.
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

// OnWaitingProcessing only logs waiting message by default.
func (d *DefaultEventHandler) OnWaitingProcessing(qf QueueFramework) {
	logs.Info(fmt.Sprintf(
		"Too many messages are processing, waiting for %d seconds.", qf.WaitProcessingSeconds(false)))
}

// OnRecoverProcessing only logs queue recovered message by default.
func (d *DefaultEventHandler) OnRecoverProcessing(_ QueueFramework) {
	logs.Info("Restart to receive messages.")
}
