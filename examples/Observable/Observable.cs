using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using NATS.Client;

namespace NATSExamples
{
    class Observable
    {
        public static void Main(string[] args)
        {
            try
            {
                new Observable().Run(args).Wait();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception: " + ex.Message);
                Console.Error.WriteLine(ex);
            }
        }

        int count = 1000000;
        string url = Defaults.Url;
        string subject = "foo";
        int received;
        bool verbose;

        public async Task Run(string[] args)
        {
            ParseArgs(args);
            Banner();
            
            var opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = url;

            using (var c = new ConnectionFactory().CreateConnection(opts))
            {
                var elapsed = await ReceiveObservable(c);

                Console.Write("Received {0} msgs in {1} seconds ", received, elapsed.TotalSeconds);
                Console.WriteLine("({0} msgs/second).", (int)(received / elapsed.TotalSeconds));
                PrintStats(c);
            }
        }

        private async Task<TimeSpan> ReceiveObservable(IConnection c)
        {
            var sw = new Stopwatch();

            await c.ToObservable(subject)
                .Take(count)
                .Do(args =>
                {
                    if (received == 0)
                        sw.Start();

                    received++;

                    if (verbose)
                        Console.WriteLine("Received: " + args.Message);
                })
                .Finally(sw.Stop)
                .ToTask();
            
            return sw.Elapsed;
        }

        private void ParseArgs(string[] args)
        {
            if (args == null)
                return;
            
            Dictionary<string, string> parsedArgs = new Dictionary<string, string>();

            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("-sync") ||
                    args[i].Equals("-verbose"))
                {
                    parsedArgs.Add(args[i], "true");
                }
                else
                {
                    if (i + 1 == args.Length)
                        Usage();

                    parsedArgs.Add(args[i], args[i + 1]);
                    i++;
                }

            }

            if (parsedArgs.ContainsKey("-count"))
                count = Convert.ToInt32(parsedArgs["-count"]);

            if (parsedArgs.ContainsKey("-url"))
                url = parsedArgs["-url"];

            if (parsedArgs.ContainsKey("-subject"))
                subject = parsedArgs["-subject"];

            if (parsedArgs.ContainsKey("-verbose"))
                verbose = true;
        }

        private void Banner()
        {
            Console.WriteLine("Receiving {0} messages on subject {1}", count, subject);
            Console.WriteLine("  Url: {0}", url);
        }

        private static void Usage()
        {
            Console.Error.WriteLine(
                "Usage:  Observable [-url url] [-subject subject] " +
                "-count [count] [-verbose]");

            Environment.Exit(-1);
        }

        private static void PrintStats(IConnection c)
        {
            var s = c.Stats;
            Console.WriteLine("Statistics:  ");
            Console.WriteLine("   Incoming Payload Bytes: {0}", s.InBytes);
            Console.WriteLine("   Incoming Messages: {0}", s.InMsgs);
        }
    }
}
