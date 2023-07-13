// Copyright 2022 The NATS Authors
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
using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATSExamples
{
    internal static class JetStreamStarter
    {        
        static readonly string Stream = "ConStream";
        static readonly string Subject = "ConSubject";
        static readonly long InactiveThreshold = 1000 * 60 * 60;
    
        static readonly StorageType StorageType = StorageType.File;
        static readonly int StreamReplicas = 3;
        static readonly int ConsumerReplicas = -1;
        static readonly ulong MessageCount = 1000;

        static void Main(string[] args)
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Name = "the-client";
            // opts.Url = "nats://localhost:4222,nats://localhost:5222,nats://localhost:6222";
            opts.Url = "nats://localhost:4222";
            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                createStream(jsm); Thread.Sleep(1000);
                for (ulong x = 0; x < MessageCount; x++) { js.Publish(Subject, Encoding.UTF8.GetBytes("data" + (x + 1))); }

                ulong mc = MessageCount;
                clearConsumers(jsm);
                string consumerName = GenerateConsumerName();
                ConsumerConfiguration.ConsumerConfigurationBuilder builder = ConsumerConfiguration.Builder()
                    .WithName(consumerName)
                    .WithInactiveThreshold(InactiveThreshold);
                if (ConsumerReplicas > 0) {
                    builder.WithNumReplicas(ConsumerReplicas);
                }

                IJetStreamPullSubscription sub = js.PullSubscribe(Subject, builder.BuildPullSubscribeOptions());
                Console.WriteLine("Consumer: " + consumerName);
                checkConsumer(jsm, consumerName, mc);
                
                Console.WriteLine("Consumer: " + consumerName);
                checkConsumer(jsm, consumerName, mc);
    
                int round = 0;
                while (true)
                {
                    Thread.Sleep(1000);
                    Console.WriteLine("\nRound " + (++round));
                    try {
                        Console.WriteLine("  Server: " + c.ServerInfo.Port);
                        Console.WriteLine("  Consumers: " + String.Join(", ", jsm.GetConsumerNames(Stream)));
                    }
                    catch (Exception e) {
                        continue;
                    }
                    checkConsumer(jsm, consumerName, mc);
                    sub.Pull(1);
                    try
                    {
                        Msg m = sub.NextMessage(1000);
                        Console.WriteLine("  Got Message");
                        m.Ack();
                        mc--;
                    }
                    catch (NATSTimeoutException)
                    {
                        Console.WriteLine("  NO MESSAGE");
                    }
                }
            }
        }
            
        private static void clearConsumers(IJetStreamManagement jsm) {
            IList<string> list = jsm.GetConsumerNames(Stream);
            foreach (string c in list) {
                try {
                    jsm.DeleteConsumer(Stream, c);
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        private static void checkConsumer(IJetStreamManagement jsm, string name, ulong pending) {
            bool active = pending > 0;
            try {
                ConsumerInfo ci = jsm.GetConsumerInfo(Stream, name);
                if (!active) {
                    Console.Error.WriteLine("  Consumer should NOT be active.");
                }
                if (pending == ci.NumPending) {
                    Console.WriteLine("  Consumer got matching pending: " + pending);
                }
                else {
                    Console.Error.WriteLine("  Incorrect pending, expected " + pending + " got " + ci.NumPending);
                }
            }
            catch (NATSJetStreamException e) {
                Console.WriteLine("  server error: " + e.Message);
                if (active) {
                    Console.Error.WriteLine("  Consumer should be active.");
                }
            }
        }

        public static void createStream(IJetStreamManagement jsm) {
            try {
                jsm.DeleteStream(Stream);
            }
            catch (Exception ignore) {}
            try {
                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(Stream)
                    .WithReplicas(StreamReplicas)
                    .WithStorageType(StorageType)
                    .WithSubjects(Subject)
                    .Build();
                jsm.AddStream(sc);
            }
            catch (Exception e) {
                Console.WriteLine("Failed creating stream: '" + Stream + "' " + e);
            }
        }    

        private static string GenerateConsumerName()
        {
            return Nuid.NextGlobalSequence();
        }
    }
}