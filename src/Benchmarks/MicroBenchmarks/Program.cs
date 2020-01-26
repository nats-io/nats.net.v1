using BenchmarkDotNet.Running;

namespace MicroBenchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<NuidBenchmark>();
        }
    }
}
