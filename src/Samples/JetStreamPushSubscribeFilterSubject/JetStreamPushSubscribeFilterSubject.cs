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
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    /// <summary>
    /// This example will demonstrate JetStream push subscribing with a filter on the subjects.
    /// </summary>
    internal static class JetStreamPushSubscribeFilterSubject
    {
        private const string Usage =
            "Usage: JetStreamPushSubscribeFilterSubject [-url url] [-creds file] [-strm stream] [-sub subject-prefix]" +
            "\n\nDefault Values:" +
            "\n   [-stream]    fs-stream" +
            "\n   [-subject]   fs-subject" +
            "\n   [-subscount] 5";
        
        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("NATS JetStream Push Subscribe Bind Durable", args, Usage)
                .DefaultStream("fs-stream")
                .DefaultSubject("fs-subject")
                .Build();

            string subjectWild = helper.Subject + ".*";
            string subjectA = helper.Subject + ".A";
            string subjectB = helper.Subject + ".B";

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Create a JetStreamManagement context.
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                    // Use the utility to create a stream stored in memory.
                    JsUtils.CreateStreamExitWhenExists(jsm, helper.Stream, subjectWild);

                    // Create our JetStream context to publish and receive JetStream messages.
                    IJetStream js = c.CreateJetStreamContext();

                    JsUtils.Publish(js, subjectA, 1);
                    JsUtils.Publish(js, subjectB, 1);
                    JsUtils.Publish(js, subjectA, 1);
                    JsUtils.Publish(js, subjectB, 1);

                    // 1. create a subscription that subscribes to the wildcard subject
                    ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                            .WithAckPolicy(AckPolicy.None) // don't want to worry about acking messages.
                            .Build();

                    PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                        .WithConfiguration(cc)
                        .Build();

                    IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(subjectWild, pso);
                    c.Flush(5000);

                    Msg m = sub.NextMessage(1000);
                    Console.WriteLine("\n1A1. Message should be from '" + subjectA + "', Sequence #1. " +
                        "It was: '" + m.Subject + "', Seq #" + m.MetaData.StreamSequence);

                    m = sub.NextMessage(1000);
                    Console.WriteLine("1B2. Message should be from '" + subjectB + "', Sequence #2. " +
                        "It was: '" + m.Subject + "', Seq #" + m.MetaData.StreamSequence);

                    m = sub.NextMessage(1000);
                    Console.WriteLine("1A3. Message should be from '" + subjectA + "', Sequence #3. " +
                        "It was: '" + m.Subject + "', Seq #" + m.MetaData.StreamSequence);

                    m = sub.NextMessage(1000);
                    Console.WriteLine("1B4. Message should be from '" + subjectB + "', Sequence #4. " +
                        "It was: '" + m.Subject + "', Seq #" + m.MetaData.StreamSequence);

                    // 2. create a subscription that subscribes only to the A subject
                    cc = ConsumerConfiguration.Builder()
                            .WithAckPolicy(AckPolicy.None) // don't want to worry about acking messages.
                            .WithFilterSubject(subjectA)
                            .Build();

                    pso = PushSubscribeOptions.Builder()
                        .WithConfiguration(cc)
                        .Build();

                    sub = js.PushSubscribeSync(subjectWild, pso);
                    c.Flush(5000);

                    m = sub.NextMessage(1000);
                    Console.WriteLine("\n2A1. Message should be from '" + subjectA + "', Sequence #1. " +
                        "It was: '" + m.Subject + "', Seq #" + m.MetaData.StreamSequence);

                    m = sub.NextMessage(1000);
                    Console.WriteLine("2A3. Message should be from '" + subjectA + "', Sequence #3. " +
                        "It was: '" + m.Subject + "', Seq #" + m.MetaData.StreamSequence);

                    try
                    {
                        sub.NextMessage(1000);
                        Console.WriteLine("2x. NOPE! Should not have gotten here");
                    }
                    catch (NATSTimeoutException) // timeout means there are no messages available
                    {
                        Console.WriteLine("2x. There was no message available.");
                    }

                    // 3. create a subscription that subscribes only to the A subject
                    cc = ConsumerConfiguration.Builder()
                            .WithAckPolicy(AckPolicy.None) // don't want to worry about acking messages.
                            .WithFilterSubject(subjectB)
                            .Build();

                    pso = PushSubscribeOptions.Builder()
                        .WithConfiguration(cc)
                        .Build();

                    sub = js.PushSubscribeSync(subjectWild, pso);
                    c.Flush(5000);

                    m = sub.NextMessage(1000);
                    Console.WriteLine("\n3A2. Message should be from '" + subjectB + "', Sequence #2. " +
                        "It was: '" + m.Subject + "', Seq #" + m.MetaData.StreamSequence);

                    m = sub.NextMessage(1000);
                    Console.WriteLine("3A4. Message should be from '" + subjectB + "', Sequence #4. " +
                        "It was: '" + m.Subject + "', Seq #" + m.MetaData.StreamSequence);

                    try
                    {
                        sub.NextMessage(1000);
                        Console.WriteLine("3x. NOPE! Should not have gotten here");
                    }
                    catch (NATSTimeoutException) // timeout means there are no messages available
                    {
                        Console.WriteLine("3x. There was no message available.");
                    }

                    Console.WriteLine();

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
