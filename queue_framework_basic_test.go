package mqpf

import (
	"errors"
	"github.com/akimimi/config_loader"
	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"testing"
)

func TestNewQueueFramework(t *testing.T) {
	config := config_loader.QueueConfig{}
	config.PollingWaitSeconds = 130
	qf := NewQueueFramework(nil, config, nil)
	if qf.GetConfig().PollingWaitSeconds != 130 {
		t.Error("queue config init failed")
	}
}

func TestQueueFramework_SetQueue(t *testing.T) {
	config := config_loader.QueueConfig{}
	queue := mockQueue{}
	qf := NewQueueFramework(nil, config, nil)
	qf.SetQueue(&queue)
	if !qf.HasValidQueue() {
		t.Error("set queue by constructor failed!")
	}
	qf = NewQueueFramework(&queue, config, nil)
	if !qf.HasValidQueue() {
		t.Error("set queue by SetQueue function failed!")
	}
}

func TestQueueFramework_SetEventHandler(t *testing.T) {
	config := config_loader.QueueConfig{}
	queue := mockQueue{}
	eventHandler := DefaultEventHandler{}
	qf := NewQueueFramework(&queue, config, nil)
	qf.SetEventHandler(&eventHandler)
	if !qf.HasEventHandler() {
		t.Error("set event handler failed!")
	}
}

func TestQueueFramework_LaunchConsumeSuccess(t *testing.T) {
	msgCount := uint64(3)
	queue := mockQueue{MaxReceived: msgCount}
	queue.CreateQPSMonitor()
	config := config_loader.QueueConfig{}
	eventHandler := DefaultEventHandler{}
	qf := NewQueueFramework(&queue, config, &eventHandler)
	queue.Qf = qf
	qf.Launch()
	stat := qf.GetStatistic()
	if stat.Fetch("msgreceived") != msgCount {
		t.Errorf("Message received count expected %d, actual %d", msgCount, stat.Fetch("msgreceived"))
	}
	if stat.Fetch("success") != msgCount {
		t.Errorf("Success count expected %d, actual %d", msgCount, stat.Fetch("success"))
	}
}

func TestQueueFramework_LaunchConsumeFailed(t *testing.T) {
	msgCount := uint64(3)
	queue := mockQueue{MaxReceived: msgCount}
	queue.CreateQPSMonitor()
	config := config_loader.QueueConfig{}
	eventHandler := mockFailedEventHandler{}
	qf := NewQueueFramework(&queue, config, &eventHandler)
	queue.Qf = qf
	qf.Launch()
	stat := qf.GetStatistic()
	if stat.Fetch("msgreceived") != msgCount {
		t.Errorf("Message received count expected %d, actual %d", msgCount, stat.Fetch("msgreceived"))
	}
	if stat.Fetch("error") != msgCount {
		t.Errorf("Error count expected %d, actual %d", msgCount, stat.Fetch("error"))
	}
}

type mockQueue struct {
	Qf *queueFramework
	QpsMonitor *ali_mns.QPSMonitor
	MaxReceived uint64
	msgReceived uint64
}

func (tq *mockQueue) CreateQPSMonitor() {
	tq.QpsMonitor = ali_mns.NewQPSMonitor(5, ali_mns.DefaultQueueQPSLimit)
}

func (tq *mockQueue) QPSMonitor() *ali_mns.QPSMonitor {
	return tq.QpsMonitor
}

func (tq *mockQueue) Name() string {
	return "test queue"
}

func (tq *mockQueue) SendMessage(message ali_mns.MessageSendRequest) (resp ali_mns.MessageSendResponse, err error) {
	return ali_mns.MessageSendResponse{}, nil
}

func (tq *mockQueue) BatchSendMessage(messages ...ali_mns.MessageSendRequest) (resp ali_mns.BatchMessageSendResponse, err error) {
	return ali_mns.BatchMessageSendResponse{}, nil
}

func (tq *mockQueue) ReceiveMessage(respChan chan ali_mns.MessageReceiveResponse, errChan chan error, waitseconds ...int64) {
	respChan <- ali_mns.MessageReceiveResponse{}
	tq.msgReceived++
	if tq.msgReceived >= tq.MaxReceived {
		tq.Qf.Stop()
	}
}

func (tq *mockQueue) BatchReceiveMessage(respChan chan ali_mns.BatchMessageReceiveResponse, errChan chan error, numOfMessages int32, waitseconds ...int64) {
}

func (tq *mockQueue) PeekMessage(respChan chan ali_mns.MessageReceiveResponse, errChan chan error) {
}

func (tq *mockQueue) BatchPeekMessage(respChan chan ali_mns.BatchMessageReceiveResponse, errChan chan error, numOfMessages int32) {
}

func (tq *mockQueue) DeleteMessage(receiptHandle string) (err error) {
	return nil
}
func (tq *mockQueue) BatchDeleteMessage(receiptHandles ...string) (resp ali_mns.BatchMessageDeleteErrorResponse, err error) {
	return ali_mns.BatchMessageDeleteErrorResponse{}, nil
}

func (tq *mockQueue) ChangeMessageVisibility(receiptHandle string, visibilityTimeout int64) (resp ali_mns.MessageVisibilityChangeResponse, err error) {
	return ali_mns.MessageVisibilityChangeResponse{}, nil
}

type mockFailedEventHandler struct {
	DefaultEventHandler
}

func (eh *mockFailedEventHandler) ConsumeMessage(body []byte, resp *ali_mns.MessageReceiveResponse) error {
	return errors.New("Mock failed event handler, consume failed!")
}
