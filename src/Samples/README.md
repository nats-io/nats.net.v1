![NATS](https://raw.githubusercontent.com/nats-io/nats.net/master/documentation/large-logo.png)

# Nats .NET Client Samples

This folder contains a number of samples:

### Regular Nats Sample Projects

1. `Publish`
1. `QueueGroup`
1. `Replier`
1. `Requestor`
1. `RxSample`
1. `Subscribe`
1. `WinFormsSample`

### JetStream Sample Projects

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
1. `JetStreamPushSubscribeFlowControl` - TBD
1. `JetStreamPushSubscribeHeartbeat` - TBD
1. `JetStreamPushSubscribeQueueDurable` - TBD push subscribing to read messages in a load balance queue using a durable consumer.

### JetStream Management / Admin Samples
1. `JetStreamManageConsumers` - demonstrate the management of consumers
1. `JetStreamManageStreams` - demonstrate the management of streams
1. `JetStreamMirrorSubUseCases` - TBD
1. `JetStreamPrefix` - TBD

### Sample Support
1. `ExampleArgs` - Helper to manage command line arguments.
1. `ExampleAuthHandler` - Example of an auth handler.
1. `ExampleUtils` - Miscellaneous utils used to start or in running examples.
1. `JetStreamExampleUtils` - Miscellaneous utils specific to JetStream examples.
