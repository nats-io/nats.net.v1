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
    [RPlotExporter]
    [MarkdownExporterAttribute.GitHub]
    [SimpleJob(RuntimeMoniker.Net462)]
    [SimpleJob(RuntimeMoniker.NetCoreApp31)]
    public class RandomBenchmark
    {
        private static readonly NUID _nuid = NUID.Instance;
        private static readonly Nuid _newNuid = new Nuid();

        [BenchmarkCategory("NextNuid")]
        [Benchmark(Baseline = true)]
        public string NUIDNext() => _nuid.Next;

        [BenchmarkCategory("NextNuid"), Benchmark]
        public string NextNuid() => _newNuid.GetNext();

    }

    public class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<RandomBenchmark>();
        }
    }
}
