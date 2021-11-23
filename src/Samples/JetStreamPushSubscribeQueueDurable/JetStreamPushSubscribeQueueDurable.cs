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
    internal static class JetStreamPushSubscribeQueueDurable
    {
        private const string Usage =
            "Usage: JetStreamPushSubscribeQueueDurable [-url url] [-creds file] [-stream stream] " +
            "[-subject subject] [-queue queue] [-durable durable] [-deliver deliverSubject] [-count count] [-subscount numsubs]" +
            "\n\nDefault Values:" +
            "\n   [-stream]    qdur-stream" +
            "\n   [-subject]   qdur-subject" +
            "\n   [-queue]     qdur-queue" +
            "\n   [-durable]   qdur-durable" +
            "\n   [-count]     100" +
            "\n   [-subscount] 5";
        
        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("NATS JetStream Push Subscribe Queue Durable", args, Usage)
                .DefaultStream("qdur-stream")
                .DefaultSubject("qdur-subject")
                .DefaultQueue("qdur-queue")
                .DefaultDurable("qdur-durable")
                .DefaultDeliverSubject("qdur-deliver")
                .DefaultCount(100)
                .DefaultSubsCount(5)
                .Build();

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Create a JetStreamManagement context.
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                    
                    // Use the utility to create a stream stored in memory.
                    JsUtils.CreateStreamExitWhenExists(jsm, helper.Stream, helper.Subject);

                    IJetStream js = c.CreateJetStreamContext();
                    
                    Console.WriteLine();

                    // create the consumer ahead of time
                    ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                        .WithDurable(helper.Durable)
                        .WithDeliverSubject(helper.DeliverSubject)
                        .WithDeliverGroup(helper.Queue)
                        .Build();
                    jsm.AddOrUpdateConsumer(helper.Stream, cc);

                    // we will just bind to that consumer
                    PushSubscribeOptions pso = PushSubscribeOptions.BindTo(helper.Stream, helper.Durable);
                    
                    InterlockedLong allReceived = new InterlockedLong();
                    IList<JsQueueSubscriber> subscribers = new List<JsQueueSubscriber>();
                    IList<Thread> subThreads = new List<Thread>();
                    for (int id = 1; id <= helper.SubsCount; id++) {
                        // setup the subscription
                        IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(helper.Subject, helper.Queue, pso);
                        
                        // create and track the runnable
                        JsQueueSubscriber qs = new JsQueueSubscriber(id, 100, js, sub, allReceived);
                        subscribers.Add(qs);
                        
                        // create, track and start the thread
                        Thread t = new Thread(qs.Run);
                        subThreads.Add(t);
                        t.Start();
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

                    // wait for all threads to finish
                    pubThread.Join(10000);
                    foreach (Thread t in subThreads)
                    {
                        t.Join(10000);
                    }

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
        int msgCount;
        IJetStreamPushSyncSubscription sub;
        InterlockedLong allReceived;
        public int received;
        public IList<string> datas;

        public JsQueueSubscriber(int id, int msgCount, IJetStream js, IJetStreamPushSyncSubscription sub, InterlockedLong allReceived)
        {
            this.id = id;
            this.msgCount = msgCount;
            this.sub = sub;
            this.allReceived = allReceived;
            received = 0;
            datas = new List<string>();
        }

        public void Report() {
            Console.WriteLine($"Sub # {id} handled {received} messages.");
        }

        public void Run()
        {
            while (allReceived.Read() < msgCount)
            {
                try
                {
                    Msg msg = sub.NextMessage(500);
                    received++;
                    allReceived.Increment();
                    datas.Add(Encoding.UTF8.GetString(msg.Data));
                    msg.Ack();
                }
                catch (NATSTimeoutException)
                {
                    // timeout is acceptable, means no messages available.
                }
            }
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
