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
using JetStreamExampleUtils;
using NATS.Client;
using NATS.Client.JetStream;
using static JetStreamExampleUtils.PrintUtils;

namespace NATSExamples
{
    class JetStreamManageConsumers
    {
        const string Usage = 
            "Usage: JetStreamManageConsumers [-url url] [-creds file] [-stream stream] " +
            "[-subject subject] [-dur durable-prefix]" +
            "\n\nDefault Values:" +
            "\n   [-stream]   mcon-stream" +
            "\n   [-subject]  mcon-subject" +
            "\n   [-dur]      mcon-durable-";

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("JetStream Manage Consumers", args, Usage)
                .Stream("mcon-stream")
                .Subject("mcon-subject")
                .Durable("mcon-durable-")
                .Build();

            try
            {
                string durable1 = helper.Durable + "1";
                string durable2 = helper.Durable + "2";

                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Create a JetStreamManagement context.
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                    // Create (add) a stream with a subject
                    Console.WriteLine("\n----------\n1. Configure And Add Stream 1");
                    StreamConfiguration streamConfig = StreamConfiguration.Builder()
                            .WithName(helper.Stream)
                            .WithSubjects(helper.Stream)
                            .WithStorageType(StorageType.Memory)
                            .Build();
                    PrintStreamInfo(jsm.AddStream(streamConfig));

                    // 1. Add Consumers
                    Console.WriteLine("\n----------\n2. Configure And Add Consumers");
                    ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                            .WithDurable(durable1) // durable name is required when creating consumers
                            .Build();
                    ConsumerInfo ci = jsm.AddOrUpdateConsumer(helper.Stream, cc);
                    PrintConsumerInfo(ci);

                    cc = ConsumerConfiguration.Builder()
                            .WithDurable(durable2)
                            .Build();
                    ci = jsm.AddOrUpdateConsumer(helper.Stream, cc);
                    PrintObject(ci);

                    // 2. Get information on consumers
                    // 2.1 get a list of all consumers
                    // 2.2 get a list of ConsumerInfo's for all consumers
                    Console.WriteLine("\n----------\n2.1 getConsumerNames");
                    IList<string> consumerNames = jsm.GetConsumerNames(helper.Stream);
                    Console.WriteLine("Consumer Names: " + String.Join(",", consumerNames)); 

                    Console.WriteLine("\n----------\n2.2 getConsumers");
                    IList<ConsumerInfo> consumers = jsm.GetConsumers(helper.Stream);
                    PrintConsumerInfoList(consumers);

                    // 3 Delete consumers
                    // Subsequent calls to deleteStream will throw a
                    // JetStreamApiException "consumer not found (404)"
                    Console.WriteLine("\n----------\n3. Delete consumers");
                    jsm.DeleteConsumer(helper.Stream, durable1);
                    consumerNames = jsm.GetConsumerNames(helper.Stream);
                    Console.WriteLine("Consumer Names: " + String.Join(",", consumerNames)); 

                    Console.WriteLine("\n----------");
                }
            }
            catch (Exception ex)
            {
                helper.ReportException(ex);
            }
        }
    }
}
