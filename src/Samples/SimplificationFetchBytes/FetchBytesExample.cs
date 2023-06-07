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
    internal static class FetchBytesExample
    {
        private static readonly string STREAM = "fetch-bytes-stream";
        private static readonly string SUBJECT = "fetch-bytes-subject";
        private static readonly string MESSAGE_TEXT = "fetch-bytes";
        private static readonly string CONSUMER_NAME_PREFIX = "fetch-bytes-consumer";
        private static readonly int MESSAGES = 20;
        private static readonly int EXPIRES_SECONDS = 2;
        public static string SERVER = "nats://localhost:4222";

        public static void Main(String[] args)
        {
            Options opts = ConnectionFactory.GetDefaultOptions(SERVER);

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                // bytes don't work before server v2.9.1
                if (c.ServerInfo.IsOlderThanVersion("2.9.1"))
                {
                    return;
                }

                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                // set's up the stream and publish data
                JsUtils.CreateOrReplaceStream(jsm, STREAM, SUBJECT);
                JsUtils.Publish(js, SUBJECT, MESSAGE_TEXT, MESSAGES, false);
                
                // Different max bytes sizes demonstrate expiration behavior

                // A. max bytes is reached before message count
                //    Each test message consumeByteCount is 138
                simpleFetch(c, js, "A", 0, 1000);

                // B. fetch max messages is reached before byte count
                //    Each test message consumeByteCount is 131 or 134
                simpleFetch(c, js, "B", 10, 2000);

                // C. fewer bytes available than the byte count
                //    Each test message consumeByteCount is 138, 140 or 141
                simpleFetch(c, js, "C", 0, 4000);
            }
        }

        private static void simpleFetch(IConnection c, IJetStream js, string label, int maxMessages, int maxBytes)
        {
            string consumerName = generateConsumerName(maxMessages, maxBytes);

            // get stream context, create consumer and get the consumer context
            IStreamContext streamContext;
            IConsumerContext consumerContext;
            try
            {
                streamContext = c.CreateStreamContext(STREAM);
                streamContext.AddConsumer(ConsumerConfiguration.Builder().WithDurable(consumerName).Build());
                consumerContext = js.CreateConsumerContext(STREAM, consumerName);
            }
            catch (Exception) {
                // possible exceptions
                // - a connection problem
                // - the stream or consumer did not exist
                return;
            }

            // Custom FetchConsumeOptions
            FetchConsumeOptions.FetchConsumeOptionsBuilder builder 
                = FetchConsumeOptions.Builder().WithExpiresIn(EXPIRES_SECONDS * 1000);
            if (maxMessages == 0)
            {
                builder.WithMaxBytes(maxBytes);
            }
            else
            {
                builder.WithMax(maxBytes, maxMessages);
            }

            FetchConsumeOptions fetchConsumeOptions = builder.Build();

            printExplanation(label, consumerName, maxMessages, maxBytes);

            Stopwatch sw = new Stopwatch();

            // create the consumer then use it
            int receivedMessages = 0;
            long receivedBytes = 0;
            using (IFetchConsumer consumer = consumerContext.Fetch(fetchConsumeOptions))
            {
                sw.Start();
                try
                {
                    while (true)
                    {
                        Msg msg = consumer.NextMessage();
                        receivedMessages++;
                        receivedBytes += msg.ConsumeByteCount;
                        msg.Ack();
                    }
                }
                catch (NATSTimeoutException)
                {
                    // normal termination of message loop
                    Console.WriteLine(
                        "!!! Timeout indicates no more messages, either due to completion or messages not available.");
                }
                catch (NATSJetStreamStatusException)
                {
                    // Either the consumer was deleted in the middle
                    // of the pull or there is a new status from the
                    // server that this client is not aware of
                }
                sw.Stop();
            }

            printSummary(receivedMessages, receivedBytes, sw.ElapsedMilliseconds);
        }

        private static string generateConsumerName(int maxMessages, int maxBytes)
        {
            if (maxMessages == 0)
            {
                return CONSUMER_NAME_PREFIX + "-" + maxBytes + "-bytes-unlimited-messages";
            }
            return CONSUMER_NAME_PREFIX + "-" + maxBytes + "-bytes-" + maxMessages + "-messages";
        }

        private static void printSummary(int receivedMessages, long receivedBytes, long elapsed)
        {
            Console.WriteLine("+++ " + receivedBytes + "/" + receivedMessages + " bytes/message(s) were received in " +
                              elapsed + "ms\n");
        }

        private static void printExplanation(string label, string name, int maxMessages, int maxBytes)
        {
            Console.WriteLine("--------------------------------------------------------------------------------");
            Console.WriteLine(label + ". " + name);
            switch (label)
            {
                case "A":
                    Console.WriteLine("=== Max bytes (" + maxBytes + ") threshold will be met since the next message would put the byte count over " + maxBytes + " bytes");
                    Console.WriteLine("=== nextMessage() will \"fast\" timeout when the fetch has been fulfilled.");
                    break;
                case "B":
                    Console.WriteLine("=== Fetch max messages (" + maxMessages + ") will be reached before max bytes (" + maxBytes + ")");
                    Console.WriteLine("=== nextMessage() will \"fast\" timeout when the fetch has been fulfilled.");
                    break;
                case "C":
                    Console.WriteLine("=== Max bytes (" + maxBytes + ") is larger than available bytes (about 2700).");
                    Console.WriteLine("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                    Console.WriteLine("=== nextMessage() blocks until expiration when there are no messages available, then times out.");
                    break;
            }
        }
    }
}
