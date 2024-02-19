// Copyright 2024 The NATS Authors
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
using System.Threading;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATSExamples
{
    public class ChaosPublisher
    {
        const string PublisherLabel = "PUBLISHER";

        readonly ChaosCommandLine cmd;
        private readonly int pubDelay;
        private PublishAck lastPa; 
        readonly InterlockedLong errorRun = new InterlockedLong(0);

        public ChaosPublisher(ChaosCommandLine cmd, int pubDelay) {
            this.cmd = cmd;
            this.pubDelay = pubDelay;
        }

        public ulong LastSeqno()
        {
            return lastPa?.Seq ?? 0;
        }
        
        public bool IsInErrorState() {
            return errorRun.Read() > 0;
        }

        public void Run()
        {
            Random r = new Random();
            bool first = true;
            long started = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            using (IConnection conn = new ConnectionFactory().CreateConnection(cmd.MakeOptions(() => PublisherLabel)))
            {
                IJetStream js = conn.CreateJetStreamContext();
                while (true)
                {
                    if (first)
                    {
                        ChaosOutput.ControlMessage(PublisherLabel, "Starting Publish");
                        first = false;
                    }
                    
                    try
                    {
                        PublishAck pa = js.Publish(cmd.Subject, null);
                        Interlocked.Exchange(ref lastPa, pa);
                        if (errorRun.Read() > 0)
                        {
                            ChaosOutput.ControlMessage(PublisherLabel, "Restarting Publish");
                        }
                        errorRun.Set(0);
                    }
                    catch (Exception e)
                    {
                        if (errorRun.Increment() == 1)
                        {
                            ChaosOutput.ControlMessage(PublisherLabel, e.Message);
                        }
                    }

                    Thread.Sleep(r.Next(0, pubDelay));
                }
            }
        }
        
        private static String uptime(long started) {
            return Duration.OfMillis(DateTimeOffset.Now.ToUnixTimeMilliseconds() - started).ToDescription().Replace("DUR", "");
        }
    }
}
