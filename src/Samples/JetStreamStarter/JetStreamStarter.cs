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
using NATS.Client;
using NATS.Client.Internals;

namespace NATSExamples
{
    internal static class JetStreamStarter
    {
        static void Main(string[] args)
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            
            opts.Name = "the-client";
            opts.Url = "nats://localhost:4222";
            
            opts.AsyncErrorEventHandler = (obj, a) =>
            {
                Console.WriteLine($"Error: {a.Error}");
            };
            opts.ReconnectedEventHandler = (obj, a) =>
            {
                Console.WriteLine($"Reconnected to {a.Conn.ConnectedUrl}");
            };
            opts.DisconnectedEventHandler = (obj, a) =>
            {
                Console.WriteLine("Disconnected.");
            };
            opts.ClosedEventHandler = (obj, a) =>
            {
                Console.WriteLine("Connection closed.");
            };
           
            Console.WriteLine($"Connecting to '{opts.Url}'");

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                Console.WriteLine("Connected.");
            }
        }
    }
}