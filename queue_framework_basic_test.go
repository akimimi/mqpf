package mqpf

import (
	"errors"
	cl "github.com/akimimi/config-loader"
	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/gogap/logs"
	"testing"
	"time"
)

func TestNewQueueFramework(t *testing.T) {
	config := cl.QueueConfig{}
	config.PollingWaitSeconds = 130
	qf := NewQueueFramework(nil, config, nil)
	if qf.GetConfig().PollingWaitSeconds != 130 {
		t.Error("queue config init failed")
	}
}

func TestQueueFramework_SetQueue(t *testing.T) {
	config := cl.QueueConfig{}
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
	config := cl.QueueConfig{}
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
	config := cl.QueueConfig{}
	config.SetDefault()
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
	config := cl.QueueConfig{MaxDequeueCount: 3}
	config.SetDefault()
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

func TestQueueFramework_ConsumeMessageTimeout(t *testing.T) {
	msgCount := uint64(3)
	queue := mockQueue{MaxReceived: msgCount}
	queue.CreateQPSMonitor()
	config := cl.QueueConfig{}
	config.SetDefault()
	config.ConsumeTimeout = 1
	config.MaxDequeueCount = 1
	eventHandler := mockLazyEventHandler{sleepSeconds: 5}
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

func TestQueueFramework_TooManyProcessingMessages(t *testing.T) {
	msgCount := uint64(2)
	queue := mockQueue{MaxReceived: msgCount}
	queue.CreateQPSMonitor()
	config := cl.QueueConfig{}
	config.SetDefault()
	config.ConsumeTimeout = 20
	config.MaxDequeueCount = 1
	config.MaxProcessingMessage = 1
	config.OverloadBreakSeconds = 10
	eventHandler := mockLazyEventHandler{sleepSeconds: 2}
	qf := NewQueueFramework(&queue, config, &eventHandler)
	queue.Qf = qf
	qf.Launch()
	stat := qf.GetStatistic()
	if stat.Fetch("msgreceived") != msgCount {
		t.Errorf("Message received count expected %d, actual %d", msgCount, stat.Fetch("msgreceived"))
	}
	if stat.Fetch("wait") != 2*(msgCount-1) {
		t.Errorf("Wait count expected %d, actual %d", 2*(msgCount-1), stat.Fetch("wait"))
	}
}

func TestQueueFramework_WaitProcessingSeconds(t *testing.T) {
	queue := mockQueue{}
	qf := NewQueueFramework(&queue, cl.QueueConfig{}, nil)
	w := qf.WaitProcessingSeconds(true)
	if w != 1 {
		t.Errorf("Waiting Seconds, expected %d, actual %d", 1, w)
	}
	w = qf.WaitProcessingSeconds(true)
	if w != 2 {
		t.Errorf("Waiting Seconds, expected %d, actual %d", 2, w)
	}
	w = qf.WaitProcessingSeconds(true)
	if w != 3 {
		t.Errorf("Waiting Seconds, expected %d, actual %d", 3, w)
	}
	w = qf.WaitProcessingSeconds(true)
	if w != 5 {
		t.Errorf("Waiting Seconds, expected %d, actual %d", 5, w)
	}
	w = qf.WaitProcessingSeconds(true)
	if w != 8 {
		t.Errorf("Waiting Seconds, expected %d, actual %d", 8, w)
	}
	w = qf.WaitProcessingSeconds(true)
	if w != 13 {
		t.Errorf("Waiting Seconds, expected %d, actual %d", 13, w)
	}
	w = qf.WaitProcessingSeconds(true)
	if w != 21 {
		t.Errorf("Waiting Seconds, expected %d, actual %d", 21, w)
	}
	w = qf.WaitProcessingSeconds(false)
	if w != 34 {
		t.Errorf("Waiting Seconds, expected %d, actual %d", 34, w)
	}
	w = qf.WaitProcessingSeconds(false)
	if w != 34 {
		t.Errorf("Waiting Seconds, expected %d, actual %d", 34, w)
	}
	qf.ResetWaitProcessingSeconds()
	w = qf.WaitProcessingSeconds(true)
	if w != 1 {
		t.Errorf("Waiting Seconds, expected %d, actual %d", 1, w)
	}
}

type mockQueue struct {
	Qf          *queueFramework
	QpsMonitor  *ali_mns.QPSMonitor
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

func (tq *mockQueue) SendMessage(_ ali_mns.MessageSendRequest) (resp ali_mns.MessageSendResponse, err error) {
	return ali_mns.MessageSendResponse{}, nil
}

func (tq *mockQueue) BatchSendMessage(_ ...ali_mns.MessageSendRequest) (resp ali_mns.BatchMessageSendResponse, err error) {
	return ali_mns.BatchMessageSendResponse{}, nil
}

func (tq *mockQueue) ReceiveMessage(respChan chan ali_mns.MessageReceiveResponse, _ chan error, _ ...int64) {
	respChan <- ali_mns.MessageReceiveResponse{}
	tq.msgReceived++
	if tq.msgReceived >= tq.MaxReceived {
		time.Sleep(300 * time.Millisecond)
		tq.Qf.Stop()
	}
}

func (tq *mockQueue) BatchReceiveMessage(_ chan ali_mns.BatchMessageReceiveResponse, _ chan error, _ int32, _ ...int64) {
}

func (tq *mockQueue) PeekMessage(_ chan ali_mns.MessageReceiveResponse, _ chan error) {
}

func (tq *mockQueue) BatchPeekMessage(_ chan ali_mns.BatchMessageReceiveResponse, _ chan error, _ int32) {
}

func (tq *mockQueue) DeleteMessage(_ string) (err error) {
	return nil
}
func (tq *mockQueue) BatchDeleteMessage(_ ...string) (resp ali_mns.BatchMessageDeleteErrorResponse, err error) {
	return ali_mns.BatchMessageDeleteErrorResponse{}, nil
}

func (tq *mockQueue) ChangeMessageVisibility(_ string, _ int64) (resp ali_mns.MessageVisibilityChangeResponse, err error) {
	return ali_mns.MessageVisibilityChangeResponse{}, nil
}

type mockFailedEventHandler struct {
	DefaultEventHandler
}

func (eh *mockFailedEventHandler) ConsumeMessage(_ []byte, _ *ali_mns.MessageReceiveResponse) error {
	return errors.New("Mock failed event handler, consume failed!")
}

type mockLazyEventHandler struct {
	sleepSeconds int
	DefaultEventHandler
}

func (e *mockLazyEventHandler) ConsumeMessage(_ []byte, _ *ali_mns.MessageReceiveResponse) error {
	logs.Info("Sleeping...")
	time.Sleep(time.Duration(e.sleepSeconds) * time.Second)
	logs.Info("Wakeup...")
	return nil
}
