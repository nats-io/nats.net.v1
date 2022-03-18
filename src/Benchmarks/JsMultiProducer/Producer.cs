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

using JsMulti;
using JsMulti.Examples;
using JsMulti.Settings;
using static JsMulti.JsMulti;

namespace JsMultiProducer
{
    public abstract class Producer
    {
        private const string Stream = "strm";
        private const string Subject = "sub";
        private const string Server = "nats://localhost:4222";

        private const bool LatencyRun = true;

        static void Main(string[] args)
        {
            Arguments a = Arguments.Instance()
                .Server(Server)
                .Subject(Subject)
                .Action(JsmAction.PubSync) // or JsmAction.PubAsync or JsmAction.PubCore for example
                .LatencyFlag(LatencyRun)
                .Threads(3)
                .IndividualConnection() // versus shared
                .ReportFrequency(10000) // report every 10K
                .Jitter(0) // > 0 means use jitter
                .MessageCount(100_000);

            a.PrintCommandLine();

            Context ctx = new Context(a);

            // latency run, the consumer code sets up the stream
            if (!LatencyRun) {
                StreamUtils.SetupStream(Stream, Subject, ctx);
            }

            Run(ctx, true, true);
        }
    }
}