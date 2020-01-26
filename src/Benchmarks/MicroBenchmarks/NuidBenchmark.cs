using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using NATS.Client;
using NATS.Client.Internals;

namespace MicroBenchmarks
{
    [DisassemblyDiagnoser(printAsm: true, printSource: true)]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [MemoryDiagnoser]
    [MarkdownExporterAttribute.GitHub]
    [SimpleJob(RuntimeMoniker.Net462)]
    [SimpleJob(RuntimeMoniker.NetCoreApp31)]
    public class NuidBenchmark
    {
        private readonly NUID _nuid = NUID.Instance;
        private readonly Nuid _newNuid = new Nuid(null, 0, 1);
        private readonly Random _random = new Random();
        public NuidBenchmark()
        {
            _nuid.Seq = 0;
        }

        [BenchmarkCategory("NextNuid")]
        [Benchmark(Baseline = true)]
        public string NUIDNext() => _nuid.Next;

        [BenchmarkCategory("NextNuid"), Benchmark]
        public string NextNuid() => _newNuid.GetNext();
    }
}
