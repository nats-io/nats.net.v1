// Copyright 2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
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
using System.Threading;
using JsMulti.Settings;
using NATS.Client;
using NATS.Client.JetStream;

namespace JsMulti.Shared
{
    public abstract class StreamUtils
    {
        public static void SetupStream(string stream, string subject, string server) {
            SetupStream(stream, subject, OptionsFactory.GetOptions(server), OptionsFactory.GetJetStreamOptions());
        }

        public static void SetupStream(string stream, Context context) {
            SetupStream(stream, context.Subject, context.GetOptions(), context.GetJetStreamOptions());
        }

        public static void SetupStream(string stream, string subject, Options options, JetStreamOptions jso) {
            using (IConnection c = new ConnectionFactory().CreateConnection(options))
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext(jso);
                try {
                    jsm.PurgeStream(stream);
                    IList<string> cons = jsm.GetConsumerNames(stream);
                    foreach (string co in cons) {
                        jsm.DeleteConsumer(stream, co);
                    }
                    Console.WriteLine("PURGED: " + jsm.GetStreamInfo(stream));
                }
                catch (NATSJetStreamException) {
                    StreamConfiguration streamConfig = StreamConfiguration.Builder()
                        .WithName(stream)
                        .WithSubjects(subject)
                        .WithStorageType(StorageType.Memory)
                        .Build();
                    Console.WriteLine("CREATED: " + jsm.AddStream(streamConfig));
                }
                Thread.Sleep(1000); // give it a little time to setup
            }
        }
    }
}