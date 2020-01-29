// Copyright 2020 The NATS Authors
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

using System.Threading;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using NATS.Client.Internals;

namespace MicroBenchmarks
{
    [MemoryDiagnoser]
    [MarkdownExporterAttribute.GitHub]
    [SimpleJob(RuntimeMoniker.Net462)]
    [SimpleJob(RuntimeMoniker.NetCoreApp31)]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
    [CategoriesColumn]
    public class InFlightRequestBenchmark
    {
        private static void OnCompleted(string _) { }

        private static CancellationToken _ct = new CancellationTokenSource().Token;

        [Params(0, 999_999)]
        public int Timeout { get; set; }

        [BenchmarkCategory("DefaultToken")]
        [Benchmark]
        public string InFlightRequest()
        {
            var request = new InFlightRequest("a", default, Timeout, OnCompleted);
            var id = request.Id;
            request.Dispose();
            return id;
        }
        [BenchmarkCategory("ClientToken")]
        [Benchmark]
        public string InFlightRequestClientToken()
        {
            var request = new InFlightRequest("a", _ct, Timeout, OnCompleted);
            var id = request.Id;
            request.Dispose();
            return id;
        }
    }
}
