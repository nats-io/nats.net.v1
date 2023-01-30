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
using System.Text;
using System.Threading;
using NATS.Client;

namespace NATSExamples
{
    abstract class ScatterGather
    {
        public static void ScatterGatherMain()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = "nats://localhost:4222";

            using (IConnection requesterConn = new ConnectionFactory().CreateConnection(opts))
            using (IConnection responderConn1 = new ConnectionFactory().CreateConnection(opts))
            using (IConnection responderConn2 = new ConnectionFactory().CreateConnection(opts))
            {
                responderConn1.SubscribeAsync("scatter", (sender, args) =>
                {
                    Console.WriteLine($"Responder A replying to request #{Encoding.UTF8.GetString(args.Message.Data)} via subject '{args.Message.Reply}'");
                    MsgHeader h = new MsgHeader { { "responderId", "A" } };
                    responderConn1.Publish(args.Message.Reply, h, args.Message.Data);
                });
        
                responderConn2.SubscribeAsync("scatter", (sender, args) =>
                {
                    Console.WriteLine($"Responder B replying to request #{Encoding.UTF8.GetString(args.Message.Data)} via subject '{args.Message.Reply}'");
                    MsgHeader h = new MsgHeader { { "responderId", "B" } };
                    responderConn2.Publish(args.Message.Reply, h, args.Message.Data);
                });

                requesterConn.SubscribeAsync("gather", (sender, args) =>
                {
                    string mId = Encoding.UTF8.GetString(args.Message.Data);
                    Console.WriteLine($"Response gathered for message {mId} received from responderId {args.Message.Header["responderId"]}.");
                });

                for (int x = 1; x <= 9; x++)
                {
                    Console.WriteLine($"\nPublishing scatter request #{x}");
                    requesterConn.Publish("scatter", "gather", Encoding.UTF8.GetBytes($"{x}"));
                    Thread.Sleep(100); // give request time to work
                }

                Console.WriteLine();
            }
        }
    }
}