using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Running;
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
    public class RandomBenchmark
    {
        private readonly NUID _nuid = NUID.Instance;
        private readonly Nuid _newNuid = new Nuid(null, 0, 1);
        private readonly Random _random = new Random();
        public RandomBenchmark()
        {
            _nuid.Seq = 0;
        }

        [BenchmarkCategory("NextNuid")]
        [Benchmark(Baseline = true)]
        public string NUIDNext() => _nuid.Next;

        [BenchmarkCategory("NextNuid"), Benchmark]
        public string NextNuid() => _newNuid.GetNext();

        [BenchmarkCategory("NextNuid"), Benchmark]
        public string OldNewInbox() {
            byte[] buf = new byte[13];
            _random.NextBytes(buf);
            return BitConverter.ToString(buf).Replace("-", "");
        }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<RandomBenchmark>();
        }
    }
}
