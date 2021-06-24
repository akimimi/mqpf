Mimit Queue Process Framework
================================================================

[![Go Reference](https://pkg.go.dev/badge/github.com/akimimi/mqpf.svg)](https://pkg.go.dev/github.com/akimimi/mqpf)
[![Build Status](https://travis-ci.com/akimimi/mqpf.svg?branch=master)](https://travis-ci.com/akimimi/mqpf)
[![Coverage Status](https://coveralls.io/repos/github/akimimi/mqpf/badge.svg?branch=master)](https://coveralls.io/github/akimimi/mqpf?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/akimimi/mqpf)](https://goreportcard.com/report/github.com/akimimi/mqpf)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


## Description
Mimit queue process framework provide a consumer frame work for task queue process.

By implement QueueEventHandlerInterface and provide the data object to QueueFramework, 
user can Launch a queue. 

User should implement ConsumeMessage function at least, as it is the entry for business logic. 
Sample code is provided in sample directory.

## Work Flow

1. Launch and invoke BeforeLaunch interface.
2. Wait for queue message.
4. Change visibility of the message for *VisibilityTimeout* seconds.
3. Invoke ParseMessageBody interface if the queue receives a message.
4. Invoke ConsumeMessage interface for user business logic.
5. If ConsumeMessage successfully disposes the message, the message will be deleted.
   Otherwise, OnConsumeFailed interface will be invoked, and the message will be visible
   after *VisibilityTimeout* seconds.
6. Go back to step 2.
7. User can stop the flow by invoke Stop interface, AfterLaunch will be invoked.

Besides the default flow, OnParseMessageBodyFailed is invoked if ParseMessageBody interface
can not decode the message correctly. 

BeforeChangeVisibility and AfterChangeVisibility are 
invoked before and after the queue changes message visibility. 

OnChangeVisibilityFailed is invoked if the queue meets error in changing message visibility. 

OnError is invoked whenever an error happens in the flow. 
User can log and process the error message.

OnWaitingMessage is invoked when the queue framework starts to wait for one queue message.

OnWaitingProcessing is invoked whenever too many messages are processing and the queue will 
wait for a few seconds. If the queue need to wait for over config.OverloadBreakSeconds,
the queue will stop itself.

OnRecoverProcessing is invoked when the queue is recovered from waiting for processing.

## Flow Control Scheme

The queue framework will wait for a few seconds, if too many messages are being processed.
When the count of processing messages is greater than QueueConfig.MaxProcessingMessage, 
a timer is triggered to wait for the queue to be more idle. The seconds to wait increases 
if the queue needs to wait for the processing continuously. 

By default, the queue will stop itself if the waiting seconds is longer than QueueConfig.OverloadBreakSeconds.