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
using System.Collections.Generic;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    internal static class SimplificationQueue
    {
        public static readonly string STREAM = "q-stream";
        public static readonly string SUBJECT = "q-subject";
        public static readonly string CONSUMER_NAME = "q-consumer";
        public static readonly int MESSAGE_COUNT = 10000;
        public static readonly int CONSUMER_COUNT = 6;
    
        public static string SERVER = "nats://localhost:4222";

        static void Main(string[] args)
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
                try
                {
                    streamContext = c.CreateStreamContext(STREAM);
                    streamContext.CreateOrUpdateConsumer(ConsumerConfiguration.Builder().WithDurable(CONSUMER_NAME).Build());
                }
                catch (Exception) {
                    // possible exceptions
                    // - a connection problem
                    // - the stream or consumer did not exist
                    return;
                }
                
                for (int x = 1; x <= MESSAGE_COUNT; x++)
                {
                    js.Publish(SUBJECT, Encoding.UTF8.GetBytes("message-" + x));
                }
                
                int readCount = MESSAGE_COUNT / CONSUMER_COUNT;
                Console.WriteLine($"Each of the {MESSAGE_COUNT} queue subscriptions will receive about {readCount} messages.");
                
                CountdownEvent latch = new CountdownEvent(MESSAGE_COUNT);
                IList<ConsumerHolder> holders = new List<ConsumerHolder>();
                for (int id = 1; id <= CONSUMER_COUNT; id++) {
                    if (id % 2 == 0) {
                        holders.Add(new HandlerConsumerHolder(id, streamContext, latch));
                    }
                    else {
                        holders.Add(new IterableConsumerHolder(id, streamContext, latch));
                    }
                }

                // wait for all messages to be received
                latch.Wait(20_000);
                    
                // report
                foreach (ConsumerHolder holder in holders) {
                    holder.Stop();
                    holder.Report();
                }
    
                Console.WriteLine();
    
                // delete the stream since we are done with it.
                c.CreateJetStreamManagementContext().DeleteStream(STREAM);
            }
        }
    }

    internal class HandlerConsumerHolder : ConsumerHolder
    {
        IMessageConsumer messageConsumer;

        public HandlerConsumerHolder(int id, IStreamContext sc, CountdownEvent latch) : base(id, sc, latch)
        {
            messageConsumer = consumerContext.Consume((s, e) =>
            {
                thisReceived++;
                latch.Signal();
                string data = Encoding.UTF8.GetString(e.Message.Data);
                Console.WriteLine($"Iterable # {id} message # {thisReceived} {data}");
                e.Message.Ack();
            });
        }
        
        public override void Stop() {
            messageConsumer.Stop(1000);
        }
    }

    internal class IterableConsumerHolder : ConsumerHolder
    {
        IIterableConsumer iterableConsumer;
        Thread t;
        CountdownEvent finished = new CountdownEvent(1);

        public IterableConsumerHolder(int id, IStreamContext sc, CountdownEvent latch) : base(id, sc, latch)
        {
            iterableConsumer = consumerContext.Consume();
            t = new Thread(() =>
            {
                while (latch.CurrentCount > 0)
                {
                    Msg msg = iterableConsumer.NextMessage(1000);
                    if (msg != null)
                    {
                        thisReceived++;
                        latch.Signal();
                        string data = Encoding.UTF8.GetString(msg.Data);
                        Console.WriteLine($"Iterable # {id} message # {thisReceived} {data}");
                        msg.Ack();
                    }
                }

                finished.Signal();
            });
            t.Start();
        }

        public override void Stop() {
            finished.Wait(2000) ; // ensures the next loop realized it could stop
        }
    }
 
    internal abstract class ConsumerHolder {
        internal int id;
        internal IConsumerContext consumerContext;
        internal int thisReceived;
        internal CountdownEvent latch;
    
        public ConsumerHolder(int id, IStreamContext sc, CountdownEvent latch) {
            this.id = id;
            thisReceived = 0;
            this.latch = latch;
            consumerContext = sc.CreateConsumerContext(SimplificationQueue.CONSUMER_NAME);
        }
    
        public void Report() {
            Console.WriteLine($"Instance # {id} handled {thisReceived} messages.");
        }

        public abstract void Stop();
    }
}