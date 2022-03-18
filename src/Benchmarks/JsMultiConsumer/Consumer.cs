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

namespace JsMultiConsumer
{
    public abstract class Consumer
    {
        private const string Stream = "strm";
        private const string Subject = "sub";
        private const string Server = "nats://localhost:4222";

        static void Main(string[] args)
        {
            Arguments a = Arguments.Instance()
                .Server(Server)
                .Subject(Subject)
                .Action(JsmAction.SubPull) // could be JsmAction.SubPull for example
                // .LatencyFlag() !!! Auto-detected for consumers
                .Threads(3)
                .IndividualConnection() // versus shared
                .ReportFrequency(10000) // report every 10K
                .Jitter(0) // > 0 means use jitter
                .BatchSize(100)
                .MessageCount(100_000)
                ;

            a.PrintCommandLine();

            Context ctx = new Context(a);

            // Consumer sets up the stream for latency (non-normal) runs.
            // Uncomment when 
            // StreamUtils.SetupStream(Stream, ctx);

            JsMultiTool.Run(ctx, true, true);
        }
    }
}