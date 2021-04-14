# Mimit Queue Process Framework

================================================================

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
   Otherwise, OnConsumeFailed interface will be invoked and the message will be visible
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


