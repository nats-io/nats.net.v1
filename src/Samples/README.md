![NATS](https://raw.githubusercontent.com/nats-io/nats.net.v1/main/documentation/large-logo.png)

# Nats .NET Client Samples

This folder contains a number of samples:

### Core Nats

1. `Publish` - A sample publisher.
1. `QueueGroup` - An example queue group subscriber.
1. `Requestor` - A requestor sample.
1. `Replier` - A sample replier for the Requestor application.
1. `RxSample` - Rx integration / functionality
1. `Subscribe` - A sample subscriber.
1. `WinFormsSample` - Use the client within a Windows application.
1. `TlsVariationsExample` - Some examples setting up the client for TLS
1. `JetStreamStarter` - A starter app for JetStream projects. 

### Simplification

Examples show the simplification JetStream stream and consume api.

1. `SimplificationContext` - Examples on how to get simplification stream and consumer contexts. 
1. `SimplificationFetchBytes` - How to fetch with byte limits.
1. `SimplificationFetchMessages` - How to fetch with message limits.
1. `SimplificationIterableConsumer` - How to endlessly consume with an iteration pattern.
1. `SimplificationMessageConsumer` - How to endless consume with an Event Handler
1. `SimplificationNext` - How to consume one message at a time.

### JetStream

1. `JetStreamPublish` - publish JetStream messages
1. `JetStreamPublishAsync` - publish JetStream messages asynchronously
1. `JetStreamPublishVsCorePublish` - publish JetStream messages versus core publish to the same stream.
1. `JetStreamPublishWithOptionsUseCases` - publish JetStream with examples on using publish options
1. `JetStreamPullSubBatchSize` - pull subscription example specifying batch size and manual handling
1. `JetStreamPullSubBatchSizeUseCases` - pull subscription example specifying batch size with examples of manual handling various cases of available messages
1. `JetStreamPullSubExpiresIn` - pull subscription example specifying expiration and manual handling
1. `JetStreamPullSubExpiresInUseCases` - pull subscription example specifying expiration with examples of manual handling various cases of available messages
1. `JetStreamPullSubFetch` - pull subscription example using fetch list macro function
1. `JetStreamPullSubFetchUseCases` - pull subscription example using fetch list macro function with examples of various cases of available messages
1. `JetStreamPullSubIterate` - pull subscription example using iterate macro function
1. `JetStreamPullSubIterateUseCases` - pull subscription example using iterate macro function with examples of various cases of available messages
1. `JetStreamPullSubNoWaitUseCases` - pull subscription example specifying no wait with examples of manual handling various cases of available messages
1. `JetStreamPushSubscribeBasicAsync` - push subscribing to read messages asynchronously and manually acknowledge messages.
1. `JetStreamPushSubscribeBasicSync` - push subscribing to read messages synchronously and manually acknowledges messages.
1. `JetStreamPushSubscribeBindDurable` - push subscribing with the bind options
1. `JetStreamPushSubscribeDeliverSubject` - push subscribing with a deliver subject and how the subject can be read as a regular Nats Message
1. `JetStreamPushSubscribeFilterSubject` - push subscribing with a filter on the subjects.
1. `JetStreamPushSubscribeQueueDurable` - push subscribing to read messages in a load balance queue using a durable consumer.

### JetStream Management / Admin
1. `JetStreamManageConsumers` - demonstrate the management of consumers
1. `JetStreamManageStreams` - demonstrate the management of streams

### Key Value
1. `KeyValueFull` -  complete example showing aspects of KV operation

### Service Framework
1. `ServiceExample` - complete example showing service running and discovery

### Miscellany

The miscellany is just a general purpose project with other examples

1. `NatsByExample` - copies of projects put into the NatsByExample website
   * `KvIntro` - introduction to Key Value
   * `PubSub` - basic publish subscribe
   * `RequestReply` - basic request reply
1. `ScatterGather` - one publish many replies.
1. `ServiceCrossClientValidator` - a testing program to validate the service api 

### Sample Support
1. `ExampleArgs` - Helper to manage command line arguments.
1. `ExampleAuthHandler` - Example of an auth handler.
1. `ExampleUtils` - Miscellaneous utils used to start or in running examples.
1. `JetStreamExampleUtils` - Miscellaneous utils specific to JetStream examples.

### Example Ideas
1. `JetStreamPushSubscribeFlowControl`
1. `JetStreamPushSubscribeHeartbeat`
1. `JetStreamMirrorSubUseCases`
1. `JetStreamPrefix`

