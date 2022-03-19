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
                .Action(JsmAction.SubPullQueue) // could be JsmAction.SubPull for example
                .MessageCount(50_000)           // default is 100_000. Consumer needs this to know when to stop.
                // .AckPolicy(AckPolicy.None)   // default is AckPolicy.Explicit which is the only policy allowed for PULL at the moment
                // .AckAllFrequency(20)         // for AckPolicy.All how many message to wait before acking, DEFAULT IS 1
                .BatchSize(20)                  // default is 10, only used with pull subs
                .Threads(3)                     // default is 1
                .IndividualConnection()         // versus .SharedConnection()
                .ReportFrequency(5000)          // default is 10_000
                ;

            a.PrintCommandLine();

            Context ctx = new Context(a);

            // Consumer sets up the stream for latency (non-normal) runs.
            // Uncomment for latency runs
            // StreamUtils.SetupStream(Stream, ctx);

            JsMultiTool.Run(ctx, true, true);
        }
    }
}