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
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    internal static class IterableConsumerExample
    {
        private static readonly string STREAM = "manually-stream";
        private static readonly string SUBJECT = "manually-subject";
        private static readonly string CONSUMER_NAME = "manually-consumer";
        private static readonly string MESSAGE_TEXT = "manually";
        private static readonly int STOP_COUNT = 500;
        private static readonly int REPORT_EVERY = 50;
        private static readonly int JITTER = 20;

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
                    consumerContext =
                        streamContext.CreateOrUpdateConsumer(ConsumerConfiguration.Builder().WithDurable(CONSUMER_NAME).Build());
                }
                catch (Exception)
                {
                    // possible exceptions
                    // - a connection problem
                    // - the stream or consumer did not exist
                    return;
                }
                
                Thread consumeThread = new Thread(() =>
                {
                    try
                    {
                        int count = 0;
                        Stopwatch sw = Stopwatch.StartNew();
                        using (IIterableConsumer consumer = consumerContext.Consume())
                        {
                            Msg msg;
                            Console.WriteLine("Starting main loop.");
                            while (count < STOP_COUNT)
                            {
                                msg = consumer.NextMessage(1000);
                                msg.Ack();
                                if (++count % REPORT_EVERY == 0)
                                {
                                    report("Main Loop Running", sw.ElapsedMilliseconds, count);
                                }
                            }

                            report("Main Loop Stopped", sw.ElapsedMilliseconds, count);

                            Console.WriteLine("Pausing for effect...allow more messages come across.");
                            Thread.Sleep(JITTER * 2); // allows more messages to come across
                            consumer.Stop(1000);

                            Console.WriteLine("Starting post-stop loop.");
                            msg = consumer.NextMessage(1000);
                            while (msg != null)
                            {
                                msg.Ack();
                                report("Post-stop loop running", sw.ElapsedMilliseconds, ++count);
                                msg = consumer.NextMessage(1000);
                            }
                        }
                        report("Done", sw.ElapsedMilliseconds, count);
                    }
                    catch (NATSJetStreamStatusException)
                    {
                        // Either the consumer was deleted in the middle
                        // of the pull or there is a new status from the
                        // server that this client is not aware of
                    }
                });
                consumeThread.Start();
    
                Publisher publisher = new Publisher(js, SUBJECT, MESSAGE_TEXT, JITTER);
                Thread pubThread = new Thread(publisher.run);
                pubThread.Start();
    
                consumeThread.Join();
                publisher.StopPublishing();
                pubThread.Join();
            }
        }

        private static void report(string label, long elapsed, int count) {
            Console.WriteLine($"{label}: Received {count} messages in {elapsed} ms.");
        }
    }
}