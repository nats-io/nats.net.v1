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
using JsMulti.Settings;
using JsMulti.Shared;

namespace JsMultiProducer
{
    public abstract class Producer
    {
        private const string Stream = "strm";
        private const string Subject = "sub";
        private const string Server = "nats://localhost:4222";

        private const bool LatencyRun = false;

        static void Main(string[] args)
        {
            Arguments a = Arguments.Instance()
                .Server(Server)
                .Subject(Subject)
                .Action(JsmAction.PubSync)  // or JsmAction.PubAsync or JsmAction.PubCore for example
                .LatencyFlag(LatencyRun)    // tells the code to add latency info to the header.
                .MessageCount(50_000)       // default is 100_000
                .PayloadSize(256)           // default is 128
                .RoundSize(50)              // how often to check Async Publish Acks, default is 100
                .Threads(3)                 // default is 1
                .IndividualConnection()     // versus .SharedConnection()
                .ReportFrequency(5000)      // default is 10_000
                ;

            a.PrintCommandLine();

            Context ctx = new Context(a);

            // Producer sets up the stream for normal (non-latency) runs.
            if (!LatencyRun) {
                StreamUtils.SetupStream(Stream, ctx);
            }

            JsMultiTool.Run(ctx, true, true);
        }
    }
}