// Copyright 2021 The NATS Authors
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
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    /// <summary>
    /// This example will demonstrate basic use of a pull subscription of:
    /// fetch pull: <c>Fetch(int batchSize, Duration or Millis maxWait)</c>
    /// </summary>
    internal static class JetStreamPullSubFetchUseCases
    {
        private const string Usage = 
            "Usage: JetStreamPullSubFetch [-url url] [-creds file] [-stream stream] [-subject subject] [-durable durable]" +
            "\n\nDefault Values:" +
            "\n   [-stream]  fetch-uc-stream" +
            "\n   [-subject] fetch-uc-subject" +
            "\n   [-durable] fetch-uc-durable";

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("Pull Subscription using primitive Expires In", args, Usage)
                .DefaultStream("fetch-uc-stream")
                .DefaultSubject("fetch-uc-subject")
                .DefaultDurable("fetch-uc-durable")
                .DefaultCount(15)
                .Build();

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Create a JetStreamManagement context.
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                    
                    // Use the utility to create a stream stored in memory.
                    JsUtils.CreateStreamExitWhenExists(jsm, helper.Stream, helper.Subject);

                    // Create our JetStream context.
                    IJetStream js = c.CreateJetStreamContext();

                    // Build our consumer configuration and subscription options.
                    // make sure the ack wait is sufficient to handle the reading and processing of the batch.
                    // Durable is REQUIRED for pull based subscriptions
                    ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                        .WithAckWait(2500)
                        .Build();
                    PullSubscribeOptions pullOptions = PullSubscribeOptions.Builder()
                        .WithDurable(helper.Durable) // required
                        .WithConfiguration(cc)
                        .Build();

                    // 0.1 Initialize. subscription
                    // 0.2 Flush outgoing communication with/to the server, useful when app is both JsUtils.Publishing and subscribing.
                    Console.WriteLine("\n----------\n0. Initialize the subscription and pull.");
                    IJetStreamPullSubscription sub = js.PullSubscribe(helper.Subject, pullOptions);
                    c.Flush(1000);

                    // 1. Fetch, but there are no messages yet.
                    // -  Read the messages, get them all (0)
                    Console.WriteLine("----------\n1. There are no messages yet");
                    IList<Msg> messages = sub.Fetch(10, 3000);
                    JsUtils.Report(messages);
                    foreach (Msg m in messages) { m.Ack(); }
                        Console.WriteLine("We should have received 0 total messages, we received: " + messages.Count);

                    // 2. Publish 10 messages
                    // -  Fetch messages, get 10
                    Console.WriteLine("----------\n2. Publish 10 which satisfies the batch");
                    JsUtils.Publish(js, helper.Subject, "A", 10);
                    messages = sub.Fetch(10, 3000);
                    JsUtils.Report(messages);
                    foreach (Msg m in messages) { m.Ack(); }
                    Console.WriteLine("We should have received 10 total messages, we received: " + messages.Count);

                    // 3. Publish 20 messages
                    // -  Fetch messages, only get 10
                    Console.WriteLine("----------\n3. Publish 20 which is larger than the batch size.");
                    JsUtils.Publish(js, helper.Subject, "B", 20);
                    messages = sub.Fetch(10, 3000);
                    JsUtils.Report(messages);
                    foreach (Msg m in messages) { m.Ack(); }
                    Console.WriteLine("We should have received 10 total messages, we received: " + messages.Count);

                    // 4. There are still messages left from the last
                    // -  Fetch messages, get 10
                    Console.WriteLine("----------\n4. Get the rest of the JsUtils.Publish.");
                    messages = sub.Fetch(10, 3000);
                    JsUtils.Report(messages);
                    foreach (Msg m in messages) { m.Ack(); }
                    Console.WriteLine("We should have received 10 total messages, we received: " + messages.Count);

                    // 5. Publish 5 messages
                    // -  Fetch messages, get 5
                    // -  Since there are less than batch size we only get what the server has.
                    Console.WriteLine("----------\n5. Publish 5 which is less than batch size.");
                    JsUtils.Publish(js, helper.Subject, "C", 5);
                    messages = sub.Fetch(10, 3000);
                    JsUtils.Report(messages);
                    foreach (Msg m in messages) { m.Ack(); }
                    Console.WriteLine("We should have received 5 total messages, we received: " + messages.Count);

                    // 6. Publish 15 messages
                    // -  Fetch messages, only get 10
                    Console.WriteLine("----------\n6. Publish 15 which is more than the batch size.");
                    JsUtils.Publish(js, helper.Subject, "D", 15);
                    messages = sub.Fetch(10, 3000);
                    JsUtils.Report(messages);
                    foreach (Msg m in messages) { m.Ack(); }
                    Console.WriteLine("We should have received 10 total messages, we received: " + messages.Count);

                    // 7. There are 5 messages left
                    // -  Fetch messages, only get 5
                    Console.WriteLine("----------\n7. There are 5 messages left.");
                    messages = sub.Fetch(10, 3000);
                    JsUtils.Report(messages);
                    foreach (Msg m in messages) { m.Ack(); }
                    Console.WriteLine("We should have received 5 messages, we received: " + messages.Count);

                    // 8. Read but don't ack.
                    // -  Fetch messages, get 10, but either take too long to ack them or don't ack them
                    Console.WriteLine("----------\n8. Fetch but don't ack.");
                    JsUtils.Publish(js, helper.Subject, "E", 10);
                    messages = sub.Fetch(10, 3000);
                    JsUtils.Report(messages);
                    Console.WriteLine("We should have received 10 message, we received: " + messages.Count);
                    Thread.Sleep(3000); // longer than the ackWait

                    // 9. Fetch messages,
                    // -  get the 10 messages we didn't ack
                    Console.WriteLine("----------\n9. Fetch, get the messages we did not ack.");
                    messages = sub.Fetch(10, 3000);
                    JsUtils.Report(messages);
                    foreach (Msg m in messages) { m.Ack(); }
                    Console.WriteLine("We should have received 10 message, we received: " + messages.Count);

                    Console.WriteLine("----------\n");

                    // delete the stream since we are done with it.
                    jsm.DeleteStream(helper.Stream);
                }
            }
            catch (Exception ex)
            {
                helper.ReportException(ex);
            }
        }
    }
}
