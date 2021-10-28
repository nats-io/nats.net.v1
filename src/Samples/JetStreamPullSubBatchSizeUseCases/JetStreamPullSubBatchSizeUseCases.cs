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
    /// This example will demonstrate miscellaneous uses cases of a pull subscription of:
    /// batch size only pull: <code>pull(int batchSize)</code>
    /// </summary>
    internal static class JetStreamPullSubBatchSizeUseCases
    {
        private const string Usage = 
            "Usage: JetStreamPullSubBatchSizeUseCases [-url url] [-creds file] [-stream stream] [-subject subject] [-durable durable]" +
            "\n\nDefault Values:" +
            "\n   [-stream]  pull-stream" +
            "\n   [-subject] pull-subject" +
            "\n   [-durable] pull-durable";

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("Pull Subscription using primitive Batch Size, Use Cases", args, Usage)
                .DefaultStream("pull-stream")
                .DefaultSubject("pull-subject")
                .DefaultDurable("pull-durable")
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
                    // 0.2 Flush outgoing communication with/to the server, useful when app is both JsUtils.Publishing and subscribing.
                    // 0.3 Start the pull, you don't have to call this again because AckMode.NEXT
                    // -  When we ack a batch message the server starts preparing or adding to the next batch.
                    Console.WriteLine("\n----------\n0. Initialize the subscription and pull.");
                    IJetStreamPullSubscription sub = js.PullSubscribe(helper.Subject, pullOptions);
                    c.Flush(1000);

                    sub.Pull(10);

                    // 1. JsUtils.Publish some that is less than the batch size.
                    // -  Do this first as data will typically be published first.
                    Console.WriteLine("----------\n1. JsUtils.Publish some amount of messages, but not entire batch size.");
                    JsUtils.Publish(js, helper.Subject, "A", 4);
                    IList<Msg> messages = JsUtils.ReadMessagesAck(sub);
                    Console.WriteLine("We should have received 4 total messages, we received: " + messages.Count);

                    // 2. JsUtils.Publish some more covering our pull size...
                    // -  Read what is available, expect only 6 b/c 4 + 6 = 10
                    Console.WriteLine("----------\n2. JsUtils.Publish more than the remaining batch size.");
                    JsUtils.Publish(js, helper.Subject, "B", 10);
                    messages = JsUtils.ReadMessagesAck(sub);
                    Console.WriteLine("We should have received 6 total messages, we received: " + messages.Count);

                    // 3. There are still 4 messages from B, but the batch was finished
                    // -  won't get any messages until a pull is issued.
                    Console.WriteLine("----------\n3. Read without re-issue.");
                    messages = JsUtils.ReadMessagesAck(sub);
                    Console.WriteLine("We should have received 0 total messages, we received: " + messages.Count);

                    // 4. re-issue the pull to get the last 4
                    Console.WriteLine("----------\n4. Re-issue to get the last 4.");
                    sub.Pull(10);
                    messages = JsUtils.ReadMessagesAck(sub);
                    Console.WriteLine("We should have received 4 total messages, we received: " + messages.Count);

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
