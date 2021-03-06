package mqpf

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"
)

// MinLogDuration is the minimum log period for top QPS monitor
const MinLogDuration = 1 // seconds

// Statistic is a struct for queue performance monitoring data
type Statistic struct {
	loop          uint64
	msgReceived   uint64
	handleSuccess uint64
	handleError   uint64
	queueError    uint64
	wait          uint64
	startAt       time.Time
	logTime       time.Time
	lastLog       *Statistic
	periodMonitor *Statistic
	duration      time.Duration
	perflog       performanceLog
}

func (ss Statistic) String() string {
	return fmt.Sprintf("Service Statistic  R: %d, S: %d, F: %d, E: %d, W: %d, I: %s",
		ss.msgReceived, ss.handleSuccess, ss.handleError, ss.queueError, ss.wait,
		ss.startAt.Format("2006-01-02 15:04:05 +0800"))
}

// Start is invoked to log when the monitoring task begins.
func (ss *Statistic) Start() {
	ss.startAt = time.Now()
}

// Loop adds 'count' for loops that fetched no messages in statistic value.
// If count is not provided, the default value for count is 1.
func (ss *Statistic) Loop(count ...uint64) {
	ss.inc(&ss.loop, count...)
}

// MessageReceived adds 'count' for received message in statistic value.
// If count is not provided, the default value for count is 1.
func (ss *Statistic) MessageReceived(count ...uint64) {
	ss.inc(&ss.msgReceived, count...)
}

// HandleSuccess adds 'count' for successful handled situation in statistic value.
// If count is not provided, the default value for count is 1.
func (ss *Statistic) HandleSuccess(count ...uint64) {
	ss.inc(&ss.handleSuccess, count...)
}

// HandleError adds 'count' for encountering problems situation in statistic value.
// If count is not provided, the default value for count is 1.
func (ss *Statistic) HandleError(count ...uint64) {
	ss.inc(&ss.handleError, count...)
}

// QueueError adds 'count' for queue problems in statistic value.
// If count is not provided, the default value for count is 1.
func (ss *Statistic) QueueError(count ...uint64) {
	ss.inc(&ss.queueError, count...)
}

// Wait adds 'count' for queue wait in statistic value.
// If count is not provided, the default value for count is 1.
func (ss *Statistic) Wait(count ...uint64) {
	ss.inc(&ss.wait, count...)
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

// Monitor update periodMonitor objects which logs a period performance for queue handling.
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

// Performance returns performance log result in string format.
func (ss *Statistic) Performance() string {
	return ss.perflog.String()
}

// MonitorLog returns period monitor log in string format.
func (ss *Statistic) MonitorLog() string {
	if ss.periodMonitor == nil {
		return ""
	}

	d := ss.periodMonitor.duration.Seconds()
	return fmt.Sprintf("Monitor R: %.3f, S: %.3f, F: %.3f, E: %.3f, N: %s",
		float64(ss.periodMonitor.msgReceived)/d,
		float64(ss.periodMonitor.handleSuccess)/d,
		float64(ss.periodMonitor.handleError)/d,
		float64(ss.periodMonitor.queueError)/d,
		ss.periodMonitor.logTime.Format("2006-01-02 15:04:05 +0800"))
}

// Fetch returns statistic value by paramName request. The function is thread safe.
func (ss *Statistic) Fetch(paramName string) uint64 {
	paramName = strings.ToLower(paramName)
	var rt uint64
	switch paramName {
	case "loop":
		rt = atomic.LoadUint64(&ss.loop)
	case "msgreceived":
		rt = atomic.LoadUint64(&ss.msgReceived)
	case "success":
		fallthrough
	case "handlesuccess":
		rt = atomic.LoadUint64(&ss.handleSuccess)
	case "error":
		fallthrough
	case "handleerror":
		rt = atomic.LoadUint64(&ss.handleError)
	case "queueerror":
		rt = atomic.LoadUint64(&ss.queueError)
	case "wait":
		rt = atomic.LoadUint64(&ss.wait)
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
	return fmt.Sprintf("Performance R: %f, S: %f, F: %f, E: %f, T: %s",
		pl.maxRecvQps, pl.maxSuccessQps, pl.maxErrorQps, pl.maxQueueErrorQps,
		pl.maxTime.Format("2006-01-02 15:04:05 +0800"))
}
