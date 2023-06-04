// Copyright 2023 The NATS Authors
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
using System.Diagnostics;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    internal static class NextExample
    {
        private static readonly string STREAM = "next-stream";
        private static readonly string SUBJECT = "next-subject";
        private static readonly string CONSUMER_NAME = "next-consumer";

        public static string SERVER = "nats://localhost:4222";

        public static void Main(String[] args)
        {
            Options opts = ConnectionFactory.GetDefaultOptions(SERVER);

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();
    
                // set's up the stream and publish data
                JsUtils.CreateOrReplaceStream(jsm, STREAM, SUBJECT);
    
                // get stream context, create consumer and get the consumer context
                IStreamContext streamContext;
                IConsumerContext consumerContext;
                try
                {
                    streamContext = c.CreateStreamContext(STREAM);
                    consumerContext = streamContext.AddConsumer(ConsumerConfiguration.Builder().WithDurable(CONSUMER_NAME).Build());
                }
                catch (Exception) {
                    // possible exceptions
                    // - a connection problem
                    // - the stream or consumer did not exist
                    return;
                }

                int count = 20;
                // Simulate messages coming in
                Thread t = new Thread(() =>
                {
                    int sleep = 2000;
                    bool down = true;
                    for (int x = 1; x <= count; x++)
                    {
                        Thread.Sleep(sleep);
                        if (down)
                        {
                            sleep -= 200;
                            down = sleep > 0;
                        }
                        else
                        {
                            sleep += 200;
                        }

                        js.Publish(SUBJECT, Encoding.UTF8.GetBytes("message-" + x));
                    }
                });
                t.Start();

                int received = 0;
                while (received < count)
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    try
                    {
                        while (true)
                        {
                            Msg msg = consumerContext.Next(1000);
                            received++;
                            msg.Ack();
                            Console.WriteLine($"Waited {sw.ElapsedMilliseconds}ms for message, got {Encoding.UTF8.GetString(msg.Data)}.");
                        }
                    }
                    catch (NATSTimeoutException)
                    {
                        // normal termination of message loop
                        Console.WriteLine($"Waited {sw.ElapsedMilliseconds}ms for message but timed out.");
                    }
                    catch (NATSJetStreamStatusException)
                    {
                        // Either the consumer was deleted in the middle
                        // of the pull or there is a new status from the
                        // server that this client is not aware of
                    }
                }

                t.Join();
            }
        }
    }
}