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
    internal static class MessageConsumerExample
    {
        private static readonly string STREAM = "consume-handler-stream";
        private static readonly string SUBJECT = "consume-handler-subject";
        private static readonly string CONSUMER_NAME = "consume-handler-consumer";
        private static readonly string MESSAGE_TEXT = "consume-handler";
        private static readonly int STOP_COUNT = 500;
        private static readonly int REPORT_EVERY = 100;
    
        private static readonly string SERVER = "nats://localhost:4222";

        public static void Main(String[] args)
        {
            Options opts = ConnectionFactory.GetDefaultOptions(SERVER);

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                // set's up the stream and publish data
                JsUtils.CreateOrReplaceStream(jsm, STREAM, SUBJECT);
                JsUtils.Publish(js, SUBJECT, MESSAGE_TEXT, 2500, false);

                // get stream context, create consumer and get the consumer context
                IStreamContext streamContext;
                IConsumerContext consumerContext;
                try
                {
                    streamContext = c.CreateStreamContext(STREAM);
                    consumerContext = streamContext.CreateOrUpdateConsumer(ConsumerConfiguration.Builder().WithDurable(CONSUMER_NAME).Build());
                }
                catch (Exception)
                {
                    // possible exceptions
                    // - a connection problem
                    // - the stream or consumer did not exist
                    return;
                }
    
                CountdownEvent latch = new CountdownEvent(1);
                int count = 0;
                Stopwatch sw = Stopwatch.StartNew();
                EventHandler<MsgHandlerEventArgs> handler = (s, e) => {
                    e.Message.Ack();
                    if (++count % REPORT_EVERY == 0) {
                        report("Handler", sw.ElapsedMilliseconds, count);
                    }
                    if (count == STOP_COUNT) {
                        latch.Signal();
                    }
                };

                using (IMessageConsumer consumer = consumerContext.StartConsume(handler))
                {
                    latch.Wait();
                    // once the consumer is stopped, the client will drain messages
                    Console.WriteLine("Stop the consumer...");
                    consumer.Stop(1000);
                    Thread.Sleep(1000); // enough for messages to drain after stop
                }

                report("Final", sw.ElapsedMilliseconds, count);
                Console.WriteLine();
            }
        }

        private static void report(string label, long elapsed, int count) {
            Console.WriteLine($"{label}: Received {count} messages in {elapsed} ms.");
        }
    }
}
