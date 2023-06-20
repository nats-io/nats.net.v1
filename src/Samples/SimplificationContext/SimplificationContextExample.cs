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
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    class SimplificationContextExample
    {
        private static readonly string STREAM = "context-stream";
        private static readonly string SUBJECT = "context-subject";
        private static readonly string CONSUMER_NAME = "context-consumer";

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
                
                // get a stream context from the connection
                IStreamContext streamContext = c.CreateStreamContext(STREAM);
                Console.WriteLine("S1. " + streamContext.GetStreamInfo());
    
                // get a stream context from the connection, supplying custom JetStreamOptions
                streamContext = c.CreateStreamContext(STREAM, JetStreamOptions.Builder().Build());
                Console.WriteLine("S2. " + streamContext.GetStreamInfo());
    
                // get a stream context from the JetStream context
                streamContext = js.CreateStreamContext(STREAM);
                Console.WriteLine("S3. " + streamContext.GetStreamInfo());
    
                // when you create a consumer from the stream context you get a ConsumerContext in return
                IConsumerContext consumerContext = streamContext.CreateOrUpdateConsumer(ConsumerConfiguration.Builder().WithDurable(CONSUMER_NAME).Build());
                Console.WriteLine("C1. " + consumerContext.GetCachedConsumerInfo());
    
                // get a ConsumerContext from the connection for a pre-existing consumer
                consumerContext = c.CreateConsumerContext(STREAM, CONSUMER_NAME);
                Console.WriteLine("C2. " + consumerContext.GetCachedConsumerInfo());
    
                // get a ConsumerContext from the connection for a pre-existing consumer, supplying custom JetStreamOptions
                consumerContext = c.CreateConsumerContext(STREAM, CONSUMER_NAME, JetStreamOptions.Builder().Build());
                Console.WriteLine("C3. " + consumerContext.GetCachedConsumerInfo());
    
                // get a ConsumerContext from the stream context for a pre-existing consumer
                consumerContext = streamContext.CreateConsumerContext(CONSUMER_NAME);
                Console.WriteLine("C4. " + consumerContext.GetCachedConsumerInfo());
            }
        }
    }
}
