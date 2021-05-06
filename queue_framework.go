package mqpf

import (
	"errors"
	"fmt"
	cl "github.com/akimimi/config_loader"
	"github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/gogap/logs"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const defaultStopQueueSeconds = 90

type QueueFramework interface {
	RegisterBreakQueueOsSingal(sigs ...os.Signal)
	GetConfig() cl.QueueConfig
	GetStatistic() *Statistic
	SetQueue(q ali_mns.AliMNSQueue)
	HasValidQueue() bool
	SetEventHandler(h QueueEventHandlerInterface)
	HasEventHandler() bool
	Launch()
	Stop()
}

type queueFramework struct {
	queue            ali_mns.AliMNSQueue
	config           cl.QueueConfig
	handler          QueueEventHandlerInterface
	breakByUser      bool
	stat             Statistic
	perfLog          performanceLog
	stopQueueSeconds int
}

func NewQueueFramework(q ali_mns.AliMNSQueue, c cl.QueueConfig, h QueueEventHandlerInterface) *queueFramework {
	qf := queueFramework{config: c, handler: &DefaultEventHandler{}, stopQueueSeconds: defaultStopQueueSeconds}
	qf.SetQueue(q)
	qf.SetEventHandler(h)
	return &qf
}

func (qf *queueFramework) GetConfig() cl.QueueConfig {
	return qf.config
}

func (qf *queueFramework) GetStatistic() *Statistic {
	return &qf.stat
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

	wg := sync.WaitGroup{} // wait for OnMessageReceived returns
	for {
		if qf.breakByUser {
			break
		}
		qf.stat.Loop()
		qf.handler.OnWaitingMessage(qf)
		endChan, respChan := make(chan int), make(chan ali_mns.MessageReceiveResponse)
		errChan := make(chan error)
		go func() {
			select {
			case resp := <-respChan:
				wg.Add(1)
				go qf.OnMessageReceived(&resp, &wg)
				endChan <- 1
			case err := <-errChan:
				qf.stat.QueueError()
				qf.handler.OnError(err, nil, nil, nil, qf)
				endChan <- 1
			}
		}()
		qf.queue.ReceiveMessage(respChan, errChan, int64(qf.config.PollingWaitSeconds))
		<-endChan
	}

	// wait for every OnMessageReceived returns in no longer than qf.stopQueueSeconds
	waitGroupFinished := make(chan bool)
	go func() {
		wg.Wait()
		waitGroupFinished <- true
	}()
	select {
	case <-waitGroupFinished:
	case <-time.After(time.Duration(qf.stopQueueSeconds) * time.Second):
	}
	qf.breakByUser = false
	qf.handler.AfterLaunch(qf)
}

func (qf *queueFramework) Stop() {
	qf.breakByUser = true
}

func (qf *queueFramework) OnMessageReceived(resp *ali_mns.MessageReceiveResponse, wg *sync.WaitGroup) {
	defer wg.Done()
	qf.stat.MessageReceived()
	qf.changeVisibility(resp, func(vret *ali_mns.MessageVisibilityChangeResponse) {
		bodyBytes, err := qf.handler.ParseMessageBody(resp)
		if err == nil {
			finishChan := make(chan error)
			if qf.GetConfig().ConsumeTimeout > 0 { // limit ConsumeMessageDuration
				go func() {
					e := qf.handler.ConsumeMessage(bodyBytes, resp)
					finishChan <- e
				}()
				select {
				case e := <-finishChan:
					err = e
				case <-time.After(time.Duration(qf.GetConfig().ConsumeTimeout) * time.Second):
					err = errors.New(
						fmt.Sprintf("ConsumeMessage timeout in %d seconds.", qf.GetConfig().ConsumeTimeout))
				}
			} else {
				err = qf.handler.ConsumeMessage(bodyBytes, resp)
			}

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
			qf.handler.OnError(err, qf.queue, resp, vret, qf)
		}
	})
}

func (qf *queueFramework) changeVisibility(resp *ali_mns.MessageReceiveResponse,
	onSuccess func(vret *ali_mns.MessageVisibilityChangeResponse)) {
	qf.handler.BeforeChangeVisibility(qf.queue, resp)
	if vret, e := qf.queue.ChangeMessageVisibility(resp.ReceiptHandle, int64(qf.config.VisibilityTimeout)); e == nil {
		qf.handler.AfterChangeVisibility(qf.queue, resp, &vret)
		onSuccess(&vret)
	} else {
		qf.stat.HandleError()
		qf.handler.OnChangeVisibilityFailed(qf.queue, resp, &vret)
		qf.handler.OnError(e, qf.queue, resp, &vret, qf)
	}
}

func (qf *queueFramework) RegisterBreakQueueOsSingal(sigs ...os.Signal) {
	signalChan := make(chan os.Signal, 1)
	if sigs == nil {
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGHUP)
	} else {
		signal.Notify(signalChan, sigs...)
	}
	go qf.listenOsSignal(signalChan)
}

func (qf *queueFramework) listenOsSignal(signalChan chan os.Signal) {
	for {
		select {
		case s := <-signalChan:
			switch s {
			case syscall.SIGINT:
				fallthrough
			case syscall.SIGTERM:
				fallthrough
			case syscall.SIGSTOP:
				logs.Info("Signal ", s.String(), " received, stopping queue daemon.....")
				qf.Stop()
			case syscall.SIGHUP:
				logs.Info(fmt.Sprintf("User Signal Received (%v)", s))
				logs.Info(qf.stat.String())
				logs.Info(qf.stat.Performance())
			}
		}
	}
}
