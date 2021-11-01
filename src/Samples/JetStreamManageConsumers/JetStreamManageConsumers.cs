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
using NATS.Client;
using NATS.Client.JetStream;
using static NATSExamples.PrintUtils;

namespace NATSExamples
{
    /// <summary>
    /// This example will demonstrate JetStream management (admin) api consumer management.
    /// </summary>
    internal static class JetStreamManageConsumers
    {
        private const string Usage = 
            "Usage: JetStreamManageConsumers [-url url] [-creds file] [-stream stream] " +
            "[-subject subject] [-durable durable-prefix]" +
            "\n\nDefault Values:" +
            "\n   [-stream]   mcon-stream" +
            "\n   [-subject]  mcon-subject" +
            "\n   [-durable]  mcon-durable-";

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("JetStream Manage Consumers", args, Usage)
                .DefaultStream("mcon-stream")
                .DefaultSubject("mcon-subject")
                .DefaultDurable("mcon-durable-")
                .Build();

            try
            {
                string durable1 = helper.Durable + "1";
                string durable2 = helper.Durable + "2";

                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Create a JetStreamManagement context.
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                    
                    // Use the utility to create a stream stored in memory.
                    JsUtils.CreateStreamExitWhenExists(jsm, helper.Stream, helper.Subject);

                    // 1. Add Consumers
                    Console.WriteLine("----------\n1. Configure And Add Consumers");
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

                    // 2. get a list of ConsumerInfo's for all consumers
                    Console.WriteLine("\n----------\n2. getConsumers");
                    IList<ConsumerInfo> consumers = jsm.GetConsumers(helper.Stream);
                    PrintConsumerInfoList(consumers);

                    // 3. get a list of all consumers
                    Console.WriteLine("\n----------\n3. getConsumerNames");
                    IList<string> consumerNames = jsm.GetConsumerNames(helper.Stream);
                    Console.WriteLine("Consumer Names: " + String.Join(",", consumerNames)); 

                    // 4. Delete a consumer, then list them again
                    // Subsequent calls to deleteStream will throw a
                    // NATSJetStreamException [10014]
                    Console.WriteLine("\n----------\n4. Delete a consumer");
                    jsm.DeleteConsumer(helper.Stream, durable1);
                    consumerNames = jsm.GetConsumerNames(helper.Stream);
                    Console.WriteLine("Consumer Names: " + String.Join(",", consumerNames)); 

                    // 5. Try to delete the consumer again and get the exception
                    Console.WriteLine("\n----------\n5. Delete consumer again");
                    try
                    {
                        jsm.DeleteConsumer(helper.Stream, durable1);
                    }
                    catch (NATSJetStreamException e)
                    {
                        Console.WriteLine($"Exception was: '{e.ErrorDescription}'");
                    }

                    Console.WriteLine("\n----------");

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
}
