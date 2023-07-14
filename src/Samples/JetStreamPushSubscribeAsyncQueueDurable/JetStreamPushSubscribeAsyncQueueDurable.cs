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
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    internal static class JetStreamPushSubscribeAsyncQueueDurable
    {
        private const string Usage =
            "Usage: JetStreamPushSubscribeAsyncQueueDurable [-url url] [-creds file] [-stream stream] " +
            "[-subject subject] [-queue queue] [-durable durable] [-deliver deliverSubject] [-count count] [-subscount numsubs]" +
            "\n\nDefault Values:" +
            "\n   [-stream]    qdur-stream" +
            "\n   [-subject]   qdur-subject" +
            "\n   [-queue]     qdur-queue" +
            "\n   [-durable]   qdur-durable" +
            "\n   [-count]     10000" +
            "\n   [-subscount] 5";
        
        // THIS FLAG WILL DETERMINE IF THE CONSUMER IS CREATED AHEAD OF TIME. SEE CODE BELOW
        static bool CreateConsumerAheadOfTime = true;

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("NATS JetStream Push Subscribe Queue Durable", args, Usage)
                .DefaultStream("qdur-stream")
                .DefaultSubject("qdur-subject")
                .DefaultQueue("qdur-queue")
                .DefaultDurable("qdur-durable")
                .DefaultDeliverSubject("qdur-deliver")
                .DefaultCount(10000)
                .DefaultSubsCount(5)
                .Build();

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Create a JetStreamManagement context.
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                    
                    // Use the utility to create a stream stored in memory.
                    JsUtils.CreateOrReplaceStream(jsm, helper.Stream, helper.Subject);

                    IJetStream js = c.CreateJetStreamContext();
                    
                    Console.WriteLine();

                    PushSubscribeOptions pso; // to be set later;

                    if (CreateConsumerAheadOfTime)
                    {
                        // CREATE THE CONSUMER AHEAD OF TIME.
                        // This is generally preferred with durable consumers.
                        ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                            .WithDurable(helper.Durable)
                            .WithDeliverSubject(helper.DeliverSubject)
                            .WithDeliverGroup(helper.Queue)
                            .WithFilterSubject(helper.Subject)
                            .Build();
                        jsm.AddOrUpdateConsumer(helper.Stream, cc);

                        // we will just bind to that consumer
                        pso = PushSubscribeOptions.BindTo(helper.Stream, helper.Durable);
                    }
                    else
                    {
                        // CREATE THE CONSUMER ON THE FLY
                        // Will only be created the first time, the subsequent subscribes will find it already made
                        pso = PushSubscribeOptions.Builder()
                            .WithConfiguration(ConsumerConfiguration.Builder()
                                .WithDurable(helper.Durable)
                                .WithDeliverGroup(helper.Queue)
                                .WithFilterSubject(helper.Subject)
                                .Build()
                            ).Build();
                    }
                    
                    CountdownEvent latch = new CountdownEvent(helper.Count);
                    IList<JsQueueSubscriber> subscribers = new List<JsQueueSubscriber>();
                    IList<IJetStreamPushAsyncSubscription> subs = new List<IJetStreamPushAsyncSubscription>();
                    int readCount = helper.Count / helper.SubsCount;
                    Console.WriteLine($"Each of the {helper.Count} queue subscriptions will receive about {readCount} messages.");

                    for (int id = 1; id <= helper.SubsCount; id++) {
                        // create and track class with the handler
                        JsQueueSubscriber qs = new JsQueueSubscriber(id, helper.Count, js, latch);
                        subscribers.Add(qs);

                        // setup the subscription
                        IJetStreamPushAsyncSubscription sub = js.PushSubscribeAsync(helper.Subject, helper.Queue,qs.Handler(), false, pso);
                        subs.Add(sub); // just keeping a reference around
                    }
                    c.Flush(500); // flush outgoing communication with/to the server

                    // create and start the publishing
                    Thread pubThread = new Thread(() =>
                    {
                        for (int x = 1; x <= helper.Count; x++)
                        {
                            js.Publish(helper.Subject, Encoding.ASCII.GetBytes("Data # " + x));
                        }
                        
                    });
                    pubThread.Start();

                    // wait for publish to finish and then wait for all messages to be received.
                    pubThread.Join(10000);
                    latch.Wait(60000);

                    foreach (JsQueueSubscriber qs in subscribers)
                    {
                        qs.Report();
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

    class JsQueueSubscriber
    {
        private int id;
        int thisReceived;
        int msgCount;
        CountdownEvent latch;
        public IList<string> datas;

        public JsQueueSubscriber(int id, int msgCount, IJetStream js, CountdownEvent latch)
        {
            this.id = id;
            this.msgCount = msgCount;
            this.latch = latch;
            thisReceived = 0;
            datas = new List<string>();
        }

        public void Report() {
            Console.WriteLine($"Sub # {id} handled {thisReceived} messages.");
        }

        public EventHandler<MsgHandlerEventArgs> Handler()
        {
            return (s, e) =>
            {
                thisReceived++;
                latch.Signal(1);
                string data = Encoding.UTF8.GetString(e.Message.Data);
                datas.Add(data);
                Console.WriteLine($"QS # {id} message # {thisReceived} {data}");
                e.Message.Ack();
            };
        }
    }

    class InterlockedLong
    {
        private long count;

        public InterlockedLong() {}

        public InterlockedLong(long start)
        {
            this.count = start;
        }

        public void Set(long l)
        {
            Interlocked.Exchange(ref count, l);
        }

        public long Increment()
        {
            return Interlocked.Increment(ref count);
        }

        public long Read()
        {
            return Interlocked.Read(ref count);
        }
    }
}
