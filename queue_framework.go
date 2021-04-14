package mqpf

import (
	cl "github.com/akimimi/config_loader"
	"github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/gogap/logs"
	"os"
	"os/signal"
	"syscall"
)

type QueueFramework interface {
	RegisterBreakQueueOsSingal(sigs ...os.Signal)
	GetConfig() cl.QueueConfig
	GetStatistic() Statistic
	SetQueue(q ali_mns.AliMNSQueue)
	HasValidQueue() bool
	SetEventHandler(h QueueEventHandlerInterface)
	HasEventHandler() bool
	Launch()
	Stop()
}

type queueFramework struct {
	queue       ali_mns.AliMNSQueue
	config      cl.QueueConfig
	handler     QueueEventHandlerInterface
	breakByUser bool
	stat        Statistic
	perfLog     performanceLog
}

func NewQueueFramework(q ali_mns.AliMNSQueue, c cl.QueueConfig, h QueueEventHandlerInterface) *queueFramework {
	qf := queueFramework{config: c, handler: &DefaultEventHandler{}}
	qf.SetQueue(q)
	qf.SetEventHandler(h)
	return &qf
}

func (qf *queueFramework) GetConfig() cl.QueueConfig {
	return qf.config
}

func (qf *queueFramework) GetStatistic() Statistic {
	return qf.stat
}

func (qf *queueFramework) SetQueue(q ali_mns.AliMNSQueue) {
	if q != nil {
		qf.queue = q
	}
}

func (qf *queueFramework) HasValidQueue() bool {
	return qf.queue != nil
}

func (qf *queueFramework) SetEventHandler(h QueueEventHandlerInterface) {
	if h != nil {
		qf.handler = h
	}
}

func (qf *queueFramework) HasEventHandler() bool {
	return qf.handler != nil
}

func (qf *queueFramework) Launch() {
	qf.handler.BeforeLaunch(qf)
	if !qf.HasValidQueue() || !qf.HasEventHandler() {
		return
	}
	for {
		// go printLogs(queue)
		if qf.breakByUser {
			break
		}
		qf.stat.Loop()
		endChan, respChan := make(chan int), make(chan ali_mns.MessageReceiveResponse)
		errChan := make(chan error)
		go func() {
			select {
			case resp := <-respChan:
				go qf.OnMessageReceived(&resp)
				endChan <- 1
			case err := <-errChan:
				qf.stat.HandleError()
				qf.handler.OnError(err, nil, nil, nil)
				endChan <- 1
			}
		}()
		qf.queue.ReceiveMessage(respChan, errChan, int64(qf.config.PollingWaitSeconds))
		<-endChan
	}
	qf.handler.AfterLaunch(qf)
	qf.breakByUser = false
}

func (qf *queueFramework) Stop() {
	qf.breakByUser = true
}

func (qf *queueFramework) OnMessageReceived(resp *ali_mns.MessageReceiveResponse) {
	qf.stat.MessageReceived()
	qf.changeVisibility(resp, func(vret *ali_mns.MessageVisibilityChangeResponse) {
		bodyBytes, err := qf.handler.ParseMessageBody(resp)
		if err == nil {
			err = qf.handler.ConsumeMessage(bodyBytes, resp)
			if err == nil {
				if err = qf.queue.DeleteMessage(vret.ReceiptHandle); err == nil {
					qf.stat.HandleSuccess()
				}
			}
		} else {
			qf.handler.OnParseMessageBodyFailed(err, resp)
		}

		if err != nil {
			qf.stat.HandleError()
			qf.handler.OnError(err, &qf.queue, resp, vret)
		}
	})
}

func (qf *queueFramework) changeVisibility(resp *ali_mns.MessageReceiveResponse,
	onSuccess func(vret *ali_mns.MessageVisibilityChangeResponse)) {
	qf.handler.BeforeChangeVisibility(&qf.queue, resp)
	if vret, e := qf.queue.ChangeMessageVisibility(resp.ReceiptHandle, int64(qf.config.VisibilityTimeout)); e == nil {
		qf.handler.AfterChangeVisibility(&qf.queue, resp, &vret)
		onSuccess(&vret)
	} else {
		qf.stat.HandleError()
		qf.handler.OnChangeVisibilityFailed(&qf.queue, resp, &vret)
		qf.handler.OnError(e, &qf.queue, resp, &vret)
	}
}

func (qf *queueFramework) RegisterBreakQueueOsSingal(sigs ...os.Signal) {
	signalChan := make(chan os.Signal, 1)
	if sigs == nil {
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	} else {
		signal.Notify(signalChan, sigs...)
	}
	go qf.listenOsSignal(signalChan)
}

func (qf *queueFramework) listenOsSignal(signalChan chan os.Signal) {
	for {
		select {
		case s := <-signalChan:
			logs.Info("Signal ", s.String(), " received, stopping queue daemon.....")
			qf.Stop()
		}
	}
}
