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
    /// fetch pull: <c>Fetch(int batchSize, Duration or Millis maxWait)</c>
    /// </summary>
    internal static class JetStreamPullSubFetch
    {
        private const string Usage = 
            "Usage: JetStreamPullSubFetch [-url url] [-creds file] [-stream stream] " +
            "[-subject subject] [-durable durable] [-count count]" +
            "\n\nDefault Values:" +
            "\n   [-stream]  fetch-stream" +
            "\n   [-subject] fetch-subject" +
            "\n   [-durable] fetch-durable" +
            "\n   [-count]   15";

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("Pull Subscription using primitive Expires In", args, Usage)
                .DefaultStream("fetch-stream")
                .DefaultSubject("fetch-subject")
                .DefaultDurable("fetch-durable")
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

                    // Start publishing the messages, don't wait for them to finish, simulating an outside producer.
                    JsUtils.PublishInBackground(js, helper.Subject, "fetch-message", helper.Count);

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

                    // subscribe
                    IJetStreamPullSubscription sub = js.PullSubscribe(helper.Subject, pullOptions);
                    c.Flush(1000);

                    int red = 0;
                    while (red < helper.Count)
                    {
                        IList<Msg> list = sub.Fetch(10, 1000);
                        foreach (Msg m in list)
                        {
                            Console.WriteLine($"{++red}. Message: {m}");
                            m.Ack();
                        }
                    }

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
