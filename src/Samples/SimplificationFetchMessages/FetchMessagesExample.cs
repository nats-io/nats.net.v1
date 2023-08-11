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
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    internal static class FetchMessagesExample
    {
        private static readonly string STREAM = "fetch-messages-stream";
        private static readonly string SUBJECT = "fetch-messages-subject";
        private static readonly string MESSAGE_TEXT = "fetch-messages";
        private static readonly string CONSUMER_NAME_PREFIX = "fetch-messages-consumer";
        private static readonly int MESSAGES = 20;
        private static readonly int EXPIRES_SECONDS = 2;
    
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
                JsUtils.Publish(js, SUBJECT, MESSAGE_TEXT, MESSAGES, false);
    
                // Different fetch max messages demonstrate expiration behavior
    
                // A. equal number of messages to the fetch max messages
                simpleFetch(c, js, "A", 20);
    
                // B. more messages than the fetch max messages
                simpleFetch(c, js, "B", 10);
    
                // C. fewer messages than the fetch max messages
                simpleFetch(c, js, "C", 40);
    
                // D. "fetch-consumer-40-messages" was created in 1C and has no messages available
                simpleFetch(c, js, "D", 40);
            }
        }

        private static void simpleFetch(IConnection c, IJetStream js, string label, int maxMessages)
        {
            string consumerName = CONSUMER_NAME_PREFIX + "-" + maxMessages + "-messages";

            // get stream context, create consumer and get the consumer context
            IStreamContext streamContext;
            IConsumerContext consumerContext;
            try
            {
                streamContext = c.GetStreamContext(STREAM);
                consumerContext =
                    streamContext.CreateOrUpdateConsumer(ConsumerConfiguration.Builder().WithDurable(consumerName)
                        .Build());
            }
            catch (Exception)
            {
                // possible exceptions
                // - a connection problem
                // - the stream or consumer did not exist
                return;
            }

            // Custom FetchConsumeOptions
            FetchConsumeOptions fetchConsumeOptions = FetchConsumeOptions.Builder()
                .WithMaxMessages(maxMessages)
                .WithExpiresIn(EXPIRES_SECONDS * 1000)
                .Build();
            printExplanation(label, consumerName, maxMessages);

            // create the consumer then use it
            try
            {
                int receivedMessages = 0;
                Stopwatch sw = Stopwatch.StartNew();
                using (IFetchConsumer consumer = consumerContext.Fetch(fetchConsumeOptions))
                {
                    Msg msg = consumer.NextMessage();
                    while (msg != null)
                    {
                        msg.Ack();
                        if (++receivedMessages == maxMessages)
                        {
                            msg = null;
                        }
                        else
                        {
                            msg = consumer.NextMessage();
                        }
                    }
                }
                sw.Stop();
                printSummary(receivedMessages, sw.ElapsedMilliseconds);
            }
            catch (NATSJetStreamStatusException)
            {
                // Either the consumer was deleted in the middle
                // of the pull or there is a new status from the
                // server that this client is not aware of
            }
        }

        private static void printSummary(int received, long elapsed) {
            Console.WriteLine("+++ " + received + " message(s) were received in " + elapsed + "ms\n");
        }
    
        private static void printExplanation(string label, string name, int maxMessages) {
            Console.WriteLine("--------------------------------------------------------------------------------");
            Console.WriteLine(label + ". " + name);
            switch (label) {
                case "A":
                case "B":
                    Console.WriteLine("=== Fetch (" + maxMessages + ") is less than or equal to available messages (" + MESSAGES + ")");
                    Console.WriteLine("=== nextMessage() will return null when consume is done");
                    break;
                case "C":
                    Console.WriteLine("=== Fetch (" + maxMessages + ") is larger than available messages (" + MESSAGES + ")");
                    Console.WriteLine("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                    Console.WriteLine("=== nextMessage() blocks until expiration when there are no messages available, then returns null.");
                    break;
                case "D":
                    Console.WriteLine("=== Fetch (" + maxMessages + ") is larger than available messages (0)");
                    Console.WriteLine("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                    Console.WriteLine("=== nextMessage() blocks until expiration when there are no messages available, then returns null.");
                    break;
            }
        }
    }
}
