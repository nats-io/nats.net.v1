using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using NATS.Client;

namespace NATSExamples
{
    internal class Observable
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

        private int _count = 1000000;
        private string _url = Defaults.Url;
        private string _subject = "foo";
        private bool _verbose;

        public async Task Run(string[] args)
        {
            ParseArgs(args);
            Banner();
            
            var opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = _url;

            using (var c = new ConnectionFactory().CreateConnection(opts))
            {
                var elapsed = await ReceiveObservable(c);

                Console.Write("Received {0} msgs in {1} seconds ", _count, elapsed.TotalSeconds);
                Console.WriteLine("({0} msgs/second).", (int)(_count / elapsed.TotalSeconds));
                PrintStats(c);
            }
        }

        private async Task<TimeSpan> ReceiveObservable(IConnection c)
        {
            var sw = new Stopwatch();

            await c.ToObservable(_subject)
                .Take(_count)
                .Do(args =>
                {
                    if (!sw.IsRunning)
                        sw.Start();

                    if (_verbose)
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
            
            var parsedArgs = new Dictionary<string, string>();

            for (var i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("-verbose"))
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
                _count = Convert.ToInt32(parsedArgs["-count"]);

            if (parsedArgs.ContainsKey("-url"))
                _url = parsedArgs["-url"];

            if (parsedArgs.ContainsKey("-subject"))
                _subject = parsedArgs["-subject"];

            if (parsedArgs.ContainsKey("-verbose"))
                _verbose = true;
        }

        private void Banner()
        {
            Console.WriteLine("Receiving {0} messages on subject {1}", _count, _subject);
            Console.WriteLine("  Url: {0}", _url);
        }

        private static void Usage()
        {
            Console.Error.WriteLine(
                "Usage:  Observable [-url url] [-subject subject] " +
                "[-count count] [-verbose]");

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
