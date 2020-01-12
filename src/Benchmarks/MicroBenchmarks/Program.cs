using System;
using System.Security.Cryptography;
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

    }

    public class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<RandomBenchmark>();
        }
    }
}
