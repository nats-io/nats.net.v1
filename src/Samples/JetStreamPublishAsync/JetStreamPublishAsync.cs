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
using System.Text;
using System.Threading.Tasks;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    /// <summary>
    /// This example will demonstrate JetStream async / future publishing.
    /// </summary>
    internal static class JetStreamPublishAsync
    {
        private const string Usage =
            "Usage: JetStreamPublishAsync [-url url] [-creds file] [-stream stream] " +
            "[-subject subject] [-count count] [-payload payload] [-header key:value]" +
            "\n\nDefault Values:" +
            "\n   [-stream]   example-stream" +
            "\n   [-subject]  example-subject" +
            "\n   [-count]    10" +
            "\n   [-payload]  Hello" +
            "\n\nRun Notes:" +
            "\n   - count < 1 is the same as 1" +
            "\n   - quote multi word payload" +
            "\n   - headers are optional, quote multi word value, no ':' in value please";

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("JetStream Publish Async", args, Usage)
                .DefaultStream("example-stream")
                .DefaultSubject("example-subject")
                .DefaultPayload("Hello")
                .DefaultCount(10)
                .Build();

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Use the utility to create a stream stored in memory.
                    JsUtils.CreateStreamOrUpdateSubjects(c, helper.Stream, helper.Subject);

                    // create a JetStream context
                    IJetStream js = c.CreateJetStreamContext();

                    IList<Task<PublishAck>> tasks = new List<Task<PublishAck>>();

                    int stop = helper.Count < 2 ? 2 : helper.Count + 1;
                    for (int x = 1; x < stop; x++)
                    {
                        // make unique message data if you want more than 1 message
                        byte[] data = helper.Count < 2
                            ? Encoding.UTF8.GetBytes(helper.Payload)
                            : Encoding.UTF8.GetBytes(helper.Payload + "-" + x);

                        // Publish a message and print the results of the publish acknowledgement.
                        Msg msg = new Msg(helper.Subject, null, helper.Header, data);

                        // We'll use the defaults for this simple example, but there are options
                        // to constrain publishing to certain streams, expect sequence numbers and
                        // more. See the JetStreamPublishWithOptionsUseCases example for details.
                        // An exception will be thrown if there is a failure.
                        tasks.Add(js.PublishAsync(msg));

                        while (tasks.Count > 0)
                        {
                            Task<PublishAck> task = tasks[0];
                            tasks.RemoveAt(0);

                            if (task.IsCompleted)
                            {
                                try
                                {
                                    PublishAck pa = task.Result;
                                    Console.WriteLine("Published message {0} on subject {1}, stream {2}, seqno {3}.",
                                        Encoding.UTF8.GetString(data), helper.Subject, pa.Stream, pa.Seq);
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine("Publish Failed: " + e);
                                }
                            }
                            else
                            {
                                // re queue so will be checked for completed again
                                tasks.Add(task);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                helper.ReportException(ex);
            }
        }
    }
}