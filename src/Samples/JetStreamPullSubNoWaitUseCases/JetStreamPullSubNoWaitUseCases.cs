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
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    /// <summary>
    /// This example will demonstrate basic use of a pull subscription of:
    /// no wait pull: <c>PullNoWait(int batchSize)</c>
    /// </summary>
    internal static class JetStreamPullSubNoWaitUseCases
    {
        private const string Usage = 
            "Usage: JetStreamPullSubNoWaitUseCases [-url url] [-creds file] [-stream stream] [-subject subject] [-durable durable]" +
            "\n\nDefault Values:" +
            "\n   [-stream]  nowait-uc-stream" +
            "\n   [-subject] nowait-uc-subject" +
            "\n   [-durable] nowait-uc-durable";

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("Pull Subscription using primitive No Wait, Use Cases", args, Usage)
                .DefaultStream("nowait-uc-stream")
                .DefaultSubject("nowait-uc-subject")
                .DefaultDurable("nowait-uc-durable")
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

                    // Build our subscription options. Durable is REQUIRED for pull based subscriptions
                    PullSubscribeOptions pullOptions = PullSubscribeOptions.Builder()
                        .WithDurable(helper.Durable) // required
                        .Build();

                    // 0.1 Initialize. subscription
                    // 0.2 DO NOT start the pull, no wait works differently than regular pull.
                    //     With no wait, we have to start the pull the first time and every time the
                    //     batch size is exhausted or no waits out.
                    // 0.3 Flush outgoing communication with/to the server, useful when app is both publishing and subscribing.
                    Console.WriteLine("\n----------\n0. Initialize the subscription and pull.");
                    IJetStreamPullSubscription sub = js.PullSubscribe(helper.Subject, pullOptions);
                    c.Flush(1000);

                    // 1. Start the pull, but there are no messages yet.
                    // -  Read the messages
                    // -  Since there are less than the batch size, we get them all (0)
                    Console.WriteLine("----------\n1. There are no messages yet");
                    sub.PullNoWait(10);
                    IList<Msg> messages = JsUtils.ReadMessagesAck(sub);
                    Console.WriteLine("We should have received 0 total messages, we received: " + messages.Count);

                    // 2. Publish 10 messages
                    // -  Start the pull
                    // -  Read the messages
                    // -  Since there are exactly the batch size we get them all
                    Console.WriteLine("----------\n2. Publish 10 which satisfies the batch");
                    JsUtils.Publish(js, helper.Subject, "A", 10);
                    sub.PullNoWait(10);
                    messages = JsUtils.ReadMessagesAck(sub);
                    Console.WriteLine("We should have received 10 total messages, we received: " + messages.Count);

                    // 3. Publish 20 messages
                    // -  Start the pull
                    // -  Read the messages
                    Console.WriteLine("----------\n3. Publish 20 which is larger than the batch size.");
                    JsUtils.Publish(js, helper.Subject, "B", 20);
                    sub.PullNoWait(10);
                    messages = JsUtils.ReadMessagesAck(sub);
                    Console.WriteLine("We should have received 10 total messages, we received: " + messages.Count);

                    // 4. There are still messages left from the last
                    // -  Start the pull
                    // -  Read the messages
                    Console.WriteLine("----------\n4. Get the rest of the publish.");
                    sub.PullNoWait(10);
                    messages = JsUtils.ReadMessagesAck(sub);
                    Console.WriteLine("We should have received 10 total messages, we received: " + messages.Count);

                    // 5. Publish 5 messages
                    // -  Start the pull
                    // -  Read the messages
                    Console.WriteLine("----------\n5. Publish 5 which is less than batch size.");
                    JsUtils.Publish(js, helper.Subject, "C", 5);
                    sub.PullNoWait(10);
                    messages = JsUtils.ReadMessagesAck(sub);
                    Console.WriteLine("We should have received 5 total messages, we received: " + messages.Count);

                    // 6. Publish 14 messages
                    // -  Start the pull
                    // -  Read the messages
                    // -  we do NOT get a nowait status message if there are more or equals messages than the batch
                    Console.WriteLine("----------\n6. Publish 14 which is more than the batch size.");
                    JsUtils.Publish(js, helper.Subject, "D", 14);
                    sub.PullNoWait(10);
                    messages = JsUtils.ReadMessagesAck(sub);
                    Console.WriteLine("We should have received 10 total messages, we received: " + messages.Count);

                    // 7. There are 4 messages left
                    // -  Start the pull
                    // -  Read the messages
                    // -  Since there are less than batch size the last message we get will be a status 404 message.
                    Console.WriteLine("----------\n7. There are 4 messages left, which is less than the batch size.");
                    sub.PullNoWait(10);
                    messages = JsUtils.ReadMessagesAck(sub);
                    Console.WriteLine("We should have received 4 messages, we received: " + messages.Count);

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
