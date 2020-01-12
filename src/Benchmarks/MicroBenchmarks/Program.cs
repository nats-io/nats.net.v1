using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Running;
using NATS.Client;

namespace MicroBenchmarks
{
    [DisassemblyDiagnoser(printAsm: true, printSource: true)]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [MemoryDiagnoser]
    [RPlotExporter]
    [MarkdownExporterAttribute.GitHub]
    [SimpleJob(RuntimeMoniker.Net462)]
    [SimpleJob(RuntimeMoniker.NetCoreApp31)]
    public class RandomBenchmark
    {
        private static readonly NUID _nuid = NUID.Instance;

        [BenchmarkCategory("NextNuid"), Benchmark(Baseline = true)]
        public string NextNuid() => _nuid.Next;

        [BenchmarkCategory("RandomizePrefix"), Benchmark(Baseline = true)]
        public void RandomizePrefix() => _nuid.RandomizePrefix();
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<RandomBenchmark>();
        }
    }
}
