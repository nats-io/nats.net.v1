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

using System.Collections.Generic;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;
using Xunit.Abstractions;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

namespace IntegrationTests
{
    public class TestJetStreamPushSyncQueue : TestSuite<JetStreamPushSyncQueueSuiteContext>
    {
        private readonly ITestOutputHelper output;

        public TestJetStreamPushSyncQueue(ITestOutputHelper output, JetStreamPushSyncQueueSuiteContext context) : base(context)
        {
            this.output = output;
        }

       [Fact]
        public void TestQueueSubWorkflow()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // Setup the subscribers
                // - the PushSubscribeOptions can be re-used since all the subscribers are the same
                // - use a concurrent integer to track all the messages received
                // - have a list of subscribers and threads so I can track them
                PushSubscribeOptions pso = PushSubscribeOptions.Builder().WithDurable(DURABLE).Build();
                InterlockedLong allReceived = new InterlockedLong();
                IList<JsQueueSubscriber> subscribers = new List<JsQueueSubscriber>();
                IList<Thread> subThreads = new List<Thread>();
                for (int id = 1; id <= 3; id++) {
                    // setup the subscription
                    IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT, QUEUE, pso);
                    // create and track the runnable
                    JsQueueSubscriber qs = new JsQueueSubscriber(100, js, sub, allReceived);
                    subscribers.Add(qs);
                    // create, track and start the thread
                    Thread t = new Thread(qs.Run);
                    subThreads.Add(t);
                    t.Start();
                }
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // create and start the publishing
                Thread pubThread = new Thread(new JsPublisher(js, 100).Run);
                pubThread.Start();

                // wait for all threads to finish
                pubThread.Join(5000);
                foreach (Thread t in subThreads) {
                    t.Join(5000);
                }

                ISet<string> uniqueDatas = new HashSet<string>();
                // count
                int count = 0;
                foreach (JsQueueSubscriber qs in subscribers) {
                    int r = qs.received;
                    Assert.True(r > 0);
                    count += r;
                    foreach (string s in qs.datas) {
                        Assert.True(uniqueDatas.Add(s));
                    }
                }

                Assert.Equal(100, count);

            });
        }
    }
    
    class JsPublisher
    {
        IJetStream js;
        int msgCount;

        public JsPublisher(IJetStream js, int msgCount)
        {
            this.js = js;
            this.msgCount = msgCount;
        }

        public void Run()
        {
            for (int x = 1; x <= msgCount; x++)
            {
                js.Publish(SUBJECT, Encoding.ASCII.GetBytes("Data # " + x));
            }
        }
    }

    class JsQueueSubscriber
    {
        int msgCount;
        IJetStream js;
        IJetStreamPushSyncSubscription sub;
        InterlockedLong allReceived;
        public int received;
        public IList<string> datas;

        public JsQueueSubscriber(int msgCount, IJetStream js, IJetStreamPushSyncSubscription sub, InterlockedLong allReceived)
        {
            this.msgCount = msgCount;
            this.js = js;
            this.sub = sub;
            this.allReceived = allReceived;
            received = 0;
            datas = new List<string>();
        }

        public void Run()
        {
            while (allReceived.Read() < msgCount)
            {
                try
                {
                    Msg msg = sub.NextMessage(500);
                    while (msg != null)
                    {
                        received++;
                        allReceived.Increment();
                        datas.Add(Encoding.UTF8.GetString(msg.Data));
                        msg.Ack();
                        msg = sub.NextMessage(500);
                    }
                }
                catch (NATSTimeoutException)
                {
                    // timeout is acceptable, means no messages available.
                }
            }
        }
    }
}