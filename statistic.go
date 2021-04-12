package mqpf

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"
)

const MinLogDuration = 1 // seconds

type Statistic struct {
	loop          uint64
	msgReceived   uint64
	handleSuccess uint64
	handleError   uint64
	queueError    uint64
	startAt       time.Time
	logTime       time.Time
	lastLog       *Statistic
	periodMonitor *Statistic
	duration      time.Duration
	perflog       performanceLog
}

func (ss Statistic) String() string {
	return fmt.Sprintf("Service Statistic  R: %d, S: %d, F: %d, E: %d, I:%s",
		ss.msgReceived, ss.handleSuccess, ss.handleError, ss.queueError,
		ss.startAt.Format("2006-01-02 15:04:05 +0800"))
}

func (ss *Statistic) Start() {
	ss.startAt = time.Now()
}

func (ss *Statistic) Loop(count ...uint64) {
	ss.inc(&ss.loop, count...)
}

func (ss *Statistic) MessageReceived(count ...uint64) {
	ss.inc(&ss.msgReceived, count...)
}

func (ss *Statistic) HandleSuccess(count ...uint64) {
	ss.inc(&ss.handleSuccess, count...)
}

func (ss *Statistic) HandleError(count ...uint64) {
	ss.inc(&ss.handleError, count...)
}

func (ss *Statistic) QueueError(count ...uint64) {
	ss.inc(&ss.queueError, count...)
}

func (ss *Statistic) inc(addr *uint64, count ...uint64) {
	if count != nil {
		for _, c := range count {
			atomic.AddUint64(addr, c)
			break
		}
	} else {
		atomic.AddUint64(addr, 1)
	}
}

func (ss *Statistic) Monitor() bool {
	updateLog := false
	if ss.lastLog == nil {
		ss.lastLog = &Statistic{}
		ss.periodMonitor = &Statistic{}
		updateLog = true
	} else {
		if time.Now().Sub(ss.lastLog.logTime).Seconds() >= MinLogDuration {
			ss.periodMonitor.loop = ss.loop - ss.lastLog.loop
			ss.periodMonitor.msgReceived = ss.msgReceived - ss.lastLog.msgReceived
			ss.periodMonitor.handleSuccess = ss.handleSuccess - ss.lastLog.handleSuccess
			ss.periodMonitor.handleError = ss.handleError - ss.lastLog.handleError
			ss.periodMonitor.queueError = ss.queueError - ss.lastLog.queueError
			ss.periodMonitor.logTime = time.Now()
			ss.periodMonitor.lastLog = nil
			ss.periodMonitor.duration = time.Now().Sub(ss.lastLog.logTime)
			ss.perflog.update(ss.periodMonitor)
			updateLog = true
		}
	}

	if updateLog {
		ss.lastLog.loop = ss.loop
		ss.lastLog.msgReceived = ss.msgReceived
		ss.lastLog.handleSuccess = ss.handleSuccess
		ss.lastLog.handleError = ss.handleError
		ss.lastLog.queueError = ss.queueError
		ss.lastLog.logTime = time.Now()
		ss.lastLog.lastLog = nil
	}
	return updateLog
}

func (ss *Statistic)Performance() string {
	return ss.perflog.String()
}

func (ss *Statistic) MonitorLog() string {
	if ss.periodMonitor == nil {
		return ""
	} else {
		d := ss.periodMonitor.duration.Seconds()
		return fmt.Sprintf("Monitor R: %.3f, S: %.3f, F: %.3f, E: %.3f, N: %s",
			float64(ss.periodMonitor.msgReceived)/d,
			float64(ss.periodMonitor.handleSuccess)/d,
			float64(ss.periodMonitor.handleError)/d,
			float64(ss.periodMonitor.queueError)/d,
			ss.periodMonitor.logTime.Format("2006-01-02 15:04:05 +0800"))
	}
}

func (ss *Statistic) Fetch(paramName string) uint64 {
	paramName = strings.ToLower(paramName)
	var rt uint64
	switch paramName {
	case "loop":
		rt = ss.loop
	case "msgreceived":
		rt = ss.msgReceived
	case "success":
		fallthrough
	case "handlesuccess":
		rt = ss.handleSuccess
	case "error":
		fallthrough
	case "handleerror":
		rt = ss.handleError
	case "queueerror":
		rt = ss.queueError
	case "duration":
		rt = uint64(ss.duration.Seconds())
	}
	return rt
}

type performanceLog struct {
	maxRecvQps       float64
	maxSuccessQps    float64
	maxErrorQps      float64
	maxQueueErrorQps float64
	maxTime          time.Time
}

func (pl *performanceLog) update(s *Statistic) {
	d := s.duration.Seconds()
	if float64(s.handleSuccess)/d > pl.maxSuccessQps {
		pl.maxTime = time.Now()
	}
	pl.maxRecvQps = math.Max(float64(s.msgReceived)/d, pl.maxRecvQps)
	pl.maxSuccessQps = math.Max(float64(s.handleSuccess)/d, pl.maxSuccessQps)
	pl.maxErrorQps = math.Max(float64(s.handleError)/d, pl.maxErrorQps)
	pl.maxQueueErrorQps = math.Max(float64(s.queueError)/d, pl.maxQueueErrorQps)
}

func (pl performanceLog) String() string {
	return fmt.Sprintf("Performance R: %f, S: %f, F: %f, E: %f, T:%s",
		pl.maxRecvQps, pl.maxSuccessQps, pl.maxErrorQps, pl.maxQueueErrorQps,
		pl.maxTime.Format("2006-01-02 15:04:05 +0800"))
}
