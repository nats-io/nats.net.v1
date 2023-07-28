// Copyright 2022-2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATSExamples.NatsByExample
{
    internal static class SimplificationMigration
    {
        public static void SimplificationMigrationMain() {
            string natsUrl = Environment.GetEnvironmentVariable("NATS_URL");
            if (natsUrl == null)
            {
                natsUrl = "nats://127.0.0.1:4222";
            }
    
            Options opts = ConnectionFactory.GetDefaultOptions(natsUrl);
            ConnectionFactory cf = new ConnectionFactory();
            using (IConnection conn = cf.CreateConnection(opts))
            {
                // ## Legacy JetStream API
                //
                // The legacy JetStream API provides two contexts both created from the Connection.
                // The `JetStream` context provides the ability to publish to streams and subscribe
                // to streams (via consumers). The `JetStreamManagement` context provides the ability
                // to manage streams and consumers themselves.
                IJetStream js = conn.CreateJetStreamContext();
                IJetStreamManagement jsm = conn.CreateJetStreamManagementContext();

                // Create a stream and populate the stream with a few messages.
                string streamName = "migration";
                jsm.AddStream(StreamConfiguration.Builder()
                    .WithName(streamName)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects("events.>")
                    .Build());

                js.Publish("events.1", null);
                js.Publish("events.2", null);
                js.Publish("events.3", null);

                // ### Continuous message retrieval with `subscribe()`
                //
                // Using the `JetStream` context, the common way to continuously receive messages is
                // to use push consumers.
                // The easiest way to create a consumer and start consuming messages
                // using the JetStream context is to use the `subscribe()` method. `subscribe()`,
                // while familiar to core NATS users, leads to complications because it will
                // create underlying consumers if they don't already exist.
                Console.WriteLine("\nA. Legacy Push Subscription with Ephemeral Consumer");

                Console.WriteLine("  Async");

                // By default, `subscribe()` performs a stream lookup by subject.
                // You can save a lookup to the server by providing the stream name in the subscribe options
                PushSubscribeOptions pushSubscribeOptions = PushSubscribeOptions.ForStream(streamName);

                IJetStreamPushAsyncSubscription asub = js.PushSubscribeAsync("events.>",
                    (s, e) =>
                    {
                        Console.WriteLine($"      Received {e.Message.Subject}");
                        e.Message.Ack();
                    },
                    false, pushSubscribeOptions);
                Thread.Sleep(100);

                // Unsubscribing this subscription will result in the underlying
                // ephemeral consumer being deleted proactively on the server.
                asub.Unsubscribe();

                Console.WriteLine("  Sync");
                IJetStreamPushSyncSubscription ssub = js.PushSubscribeSync("events.>", pushSubscribeOptions);
                while (true)
                {
                    try
                    {
                        Msg msg = ssub.NextMessage(100);
                        Console.WriteLine($"      Read {msg.Subject}");
                        msg.Ack();
                    }
                    catch (NATSTimeoutException)
                    {
                        // done in our case
                        break;
                    }
                }

                ssub.Unsubscribe();

                // ### Binding to an existing consumer
                //
                // In order to create a consumer outside the `subscribe` method,
                // the `JetStreamManagement` context `addOrUpdateConsumer` method can be used.
                // If a durable is not provided, the consumer will be ephemeral and will
                // be deleted if it becomes inactive for longer than the inactivity threshold.
                // If neither `durable` nor `name` are not provided, the client will generate a name
                // that can be found via `ConsumerInfo.getName()`
                Console.WriteLine("\nB. Legacy Bind Subscription to Named Consumer.");
                ConsumerConfiguration consumerConfiguration = ConsumerConfiguration.Builder()
                    .WithDeliverSubject("deliverB") // required for push consumers
                    .WithAckPolicy(AckPolicy.Explicit)
                    .WithInactiveThreshold(Duration.OfMinutes(10))
                    .Build();

                ConsumerInfo consumerInfo = jsm.AddOrUpdateConsumer(streamName, consumerConfiguration);
                asub = js.PushSubscribeAsync("events.>",
                    (s, e) =>
                    {
                        Console.WriteLine($"   Received {e.Message.Subject}");
                        e.Message.Ack();
                    },
                    false, PushSubscribeOptions.BindTo(streamName, consumerInfo.Name));

                Thread.Sleep(100);
                asub.Unsubscribe();

                // ### Pull consumers
                //
                // The `JetStream` context API also supports pull consumers.
                // Using pull consumers requires more effort on the developer's side
                // than push consumers to maintain an endless stream of messages.
                // Batches of messages can be retrieved using the `Fetch` method.
                // Fetch blocks until the batch size is fulfilled or until the time expires
                Console.WriteLine("\nC. Legacy Pull Subscription then Iterate");
                PullSubscribeOptions pullSubscribeOptions = PullSubscribeOptions.Builder().Build();
                IJetStreamPullSubscription usub = js.PullSubscribe("events.>", pullSubscribeOptions);

                Stopwatch sw = Stopwatch.StartNew();
                IList<Msg> messages = usub.Fetch(10, 2000);
                sw.Stop();
                Console.WriteLine($"   The call to `Fetch(10, 2000)` returned in {sw.ElapsedMilliseconds}ms.");
                foreach (Msg msg in messages)
                {
                    Console.WriteLine($"   Processing {msg.Subject}.");
                    msg.Ack();
                }

                // ## Simplified JetStream API
                //
                // The simplified API has a `StreamContext` for accessing existing
                // streams, creating consumers, and getting a `ConsumerContext`.
                // The `StreamContext` can be created from the `Connection` similar to
                // the legacy API.
                Console.WriteLine("\nD. Simplification StreamContext");
                IStreamContext streamContext = conn.CreateStreamContext(streamName);
                StreamInfo streamInfo = streamContext.GetStreamInfo(StreamInfoOptions.Builder().WithAllSubjects().Build());

                Console.WriteLine($"   Stream Name: {streamInfo.Config.Name}");
                Console.WriteLine($"   Stream Subjects: [{string.Join(",",streamInfo.State.Subjects)}]");
    
                // ### Creating a consumer from the stream context
                //
                // To create an ephemeral consumer, the `CreateOrUpdateConsumer` method
                // can be used with a bare `ConsumerConfiguration` object.
                Console.WriteLine("\nE. Simplification, Create a Consumer");
                consumerConfiguration = ConsumerConfiguration.Builder().Build();
                IConsumerContext consumerContext = streamContext.CreateOrUpdateConsumer(consumerConfiguration);
                consumerInfo = consumerContext.GetCachedConsumerInfo();
                string consumerName = consumerInfo.Name;
                Console.WriteLine($"   A consumer was created on stream \"{consumerInfo.Stream}\"");
                Console.WriteLine($"   The consumer name is \"{consumerInfo.Name}\".");
                Console.WriteLine($"   The consumer has {consumerInfo.NumPending} messages available.");
    
                // ### Getting a consumer from the stream context
                //
                // If your consumer already exists as a durable, you can create a
                // `ConsumerContext` for that consumer from the stream context or directly
                // from the connection by providing the stream and consumer name.
                consumerContext = streamContext.CreateConsumerContext(consumerName);
                consumerInfo = consumerContext.GetCachedConsumerInfo();
                Console.WriteLine($"   The ConsumerContext for \"{consumerInfo.Name}\" was loaded from the StreamContext for \"{consumerInfo.Stream}\"");
    
                consumerContext = conn.CreateConsumerContext(streamName, consumerName);
                consumerInfo = consumerContext.GetCachedConsumerInfo();
                Console.WriteLine($"   The ConsumerContext for \"{consumerInfo.Name}\" was loaded from the Connection on the stream \"{consumerInfo.Stream}\"");
    
                // ### Continuous message retrieval with `consume()`
                //
                // In order to continuously receive messages, the `consume` method
                // can be used with or without a `MessageHandler`. These methods work
                // similarly to the push `subscribe` methods used to receive messages.
                //
                // `consume` (and other ConsumerContext methods) never create a consumer
                // instead always using a consumer created previously.
                // <!break>
    
    
                // #### MessageConsumer
                // A `MessageConsumer` is returned when you call the `consume` method passing
                // `MessageHandler` on `ConsumerContext`.
                // Auto *ack* is no longer an option when a handler is provided to avoid
                // confusion. It is the developer's responsibility to ack or not based on
                // the consumer's ack policy. Ack policy is "explicit" if not otherwise set.
                //
                // Remember, when you have a handler and message are sent asynchronously,
                // make sure you have set up your error handler.
                Console.WriteLine("\nF. MessageConsumer (endless consumer with handler)");
                consumerConfiguration = ConsumerConfiguration.Builder().Build();
                consumerContext = streamContext.CreateOrUpdateConsumer(consumerConfiguration);
                consumerInfo = consumerContext.GetCachedConsumerInfo();
    
                Console.WriteLine($"   A consumer was created on stream \"{consumerInfo.Stream}\"");
                Console.WriteLine($"   The consumer name is \"{consumerInfo.Name}\".");
                Console.WriteLine($"   The consumer has {consumerInfo.NumPending} messages available.");
    
                IMessageConsumer messageConsumer = consumerContext.StartConsume(
                    (s, e) => 
                    {
                        Console.WriteLine($"   Received {e.Message.Subject}");
                        e.Message.Ack();
                    });
                Thread.Sleep(100);
    
                // To stop the consumer, the `stop` on `MessageConsumer` can be used.
                // In contrast to `unsubscribe()` in the legacy API, this will not proactively
                // delete the consumer.
                // However, the consumer will be automatically deleted by the server when the
                // `inactiveThreshold` is reached.
                messageConsumer.Stop(100);
                Console.WriteLine("   stop was called.");
    
                // #### IterableConsumer
                // An `IterableConsumer` is returned when you call the `consume` method on
                // the `ConsumerContext` *without* supplying a message handler.
                Console.WriteLine("\nG. IterableConsumer (endless consumer manually calling next)");
                consumerConfiguration = ConsumerConfiguration.Builder().Build();
                consumerContext = streamContext.CreateOrUpdateConsumer(consumerConfiguration);
                consumerInfo = consumerContext.GetCachedConsumerInfo();
    
                Console.WriteLine($"   A consumer was created on stream \"{consumerInfo.Stream}\"");
                Console.WriteLine($"   The consumer name is \"{consumerInfo.Name}\".");
                Console.WriteLine($"   The consumer has {consumerInfo.NumPending} messages available.");
    
                // Notice the `nextMessage` method can throw a `JetStreamStatusCheckedException`.
                // Under the covers the `IterableConsumer` is handling more than just messages.
                // It handles information from the server regarding the status of the underlying
                // operations. For instance, it is possible, but unlikely, that the consumer
                // could be deleted by another application in your ecosystem and if that happens
                // in the middle of the consumer, the exception would be thrown.
                IIterableConsumer iterableConsumer = consumerContext.StartIterate();
                for (int x = 0; x < 3; x++) {
                    Msg msg1 = iterableConsumer.NextMessage(100);
                    Console.WriteLine($"   Received {msg1.Subject}");
                    msg1.Ack();
                }
                iterableConsumer.Stop(100);
                Console.WriteLine("   stop was called.");
    
                // ### Retrieving messages on demand with `fetch` and `next`
    
                // #### FetchConsumer
                // A `FetchConsumer` is returned when you call the `fetch` methods on `ConsumerContext`.
                // You will use that object to call `nextMessage`.
                // Notice there is no stop on the `FetchConsumer` interface, the fetch stops by itself.
                // The new version of fetch is very similar to the old iterate, as it does not block
                // before returning the entire batch.
                Console.WriteLine("\nH. FetchConsumer (bounded consumer)");
                consumerConfiguration = ConsumerConfiguration.Builder().Build();
                consumerContext = streamContext.CreateOrUpdateConsumer(consumerConfiguration);
                consumerInfo = consumerContext.GetCachedConsumerInfo();
    
                Console.WriteLine($"   A consumer was created on stream \"{consumerInfo.Stream}\"");
                Console.WriteLine($"   The consumer name is \"{consumerInfo.Name}\".");
                Console.WriteLine($"   The consumer has {consumerInfo.NumPending} messages available.");
    
                sw = Stopwatch.StartNew();
                IFetchConsumer fetchConsumer = consumerContext.FetchMessages(2);
                Console.WriteLine($"   'Fetch' returned in {sw.ElapsedMilliseconds}ms.");
    
                // `Fetch` will return null once there are no more messages to consume.
                Msg msg2 = fetchConsumer.NextMessage();
                while (msg2 != null) {
                    Console.WriteLine($"   Processing {msg2.Subject} {sw.ElapsedMilliseconds}ms after start.");
                    msg2.Ack();
                    msg2 = fetchConsumer.NextMessage();
                }
                sw.Stop();
                Console.WriteLine($"   Fetch complete in {sw.ElapsedMilliseconds}ms.");
    
                // #### next
                // The `next` method can be used to retrieve a single
                // message, as if you had called the old fetch or iterate with a batch size of 1.
                // The minimum wait time when calling next is 1 second (1000ms)
                Console.WriteLine("\nI. next (1 message)");
                Msg msg3 = consumerContext.Next(1000);
                Console.WriteLine($"   Received {msg3.Subject}");
                msg3.Ack();
            }
        }
    }
}