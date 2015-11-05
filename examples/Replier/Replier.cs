// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using NATS.Client;


namespace NATSExamples
{
    class Replier
    {
        Dictionary<string, string> parsedArgs = new Dictionary<string, string>();

        int count = 20000;
        string url = Defaults.Url;
        string subject = "foo";
        bool sync = false;
        int received = 0;
        bool verbose = false;
        Msg replyMsg = new Msg();

        public void Run(string[] args)
        {
            parseArgs(args);
            banner();

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = url;

            replyMsg.Data = Encoding.UTF8.GetBytes("reply"); 

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                TimeSpan elapsed;

                if (sync)
                {
                    elapsed = receiveSyncSubscriber(c);
                }
                else
                {
                    elapsed = receiveAsyncSubscriber(c);
                }

                System.Console.Write("Replied to {0} msgs in {1} seconds ", count, elapsed.TotalSeconds);
                System.Console.WriteLine("({0} replies/second).",
                    (int)(count / elapsed.TotalSeconds));
                printStats(c);

            }
        }

        private void printStats(IConnection c)
        {
            IStatistics s = c.Stats;
            System.Console.WriteLine("Statistics:  ");
            System.Console.WriteLine("   Incoming Payload Bytes: {0}", s.InBytes);
            System.Console.WriteLine("   Incoming Messages: {0}", s.InMsgs);
            System.Console.WriteLine("   Outgoing Payload Bytes: {0}", s.OutBytes);
            System.Console.WriteLine("   Outgoing Messages: {0}", s.OutMsgs);
        }


        private TimeSpan receiveAsyncSubscriber(IConnection c)
        {
            Stopwatch sw = new Stopwatch();

            using (IAsyncSubscription s = c.SubscribeAsync(subject))
            {
                Object testLock = new Object();

                s.MessageHandler += (sender, args) =>
                {
                    if (received == 0)
                        sw.Start();

                    received++;

                    if (verbose)
                        Console.WriteLine("Received: " + args.Message);

                    replyMsg.Subject = args.Message.Reply;
                    c.Publish(replyMsg);

                    if (received >= count)
                    {
                        sw.Stop();
                        lock (testLock)
                        {
                            Monitor.Pulse(testLock);
                        }
                    }
                };

                lock (testLock)
                {
                    s.Start();
                    Monitor.Wait(testLock);
                }
            }

            return sw.Elapsed;
        }


        private TimeSpan receiveSyncSubscriber(IConnection c)
        {
            using (ISyncSubscription s = c.SubscribeSync(subject))
            {
                s.NextMessage();
                received++;

                Stopwatch sw = Stopwatch.StartNew();

                while (received < count)
                {
                    received++;
                    Msg m = s.NextMessage();
                    if (verbose)
                        Console.WriteLine("Received: " + m);

                    replyMsg.Subject = m.Reply;
                    c.Publish(replyMsg);
                }

                sw.Stop();
                return sw.Elapsed;
            }
        }

        private void usage()
        {
            System.Console.Error.WriteLine(
                "Usage:  Publish [-url url] [-subject subject] " +
                "-count [count] [-sync] [-verbose]");

            System.Environment.Exit(-1);
        }

        private void parseArgs(string[] args)
        {
            if (args == null)
                return;

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
                        usage();

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

            if (parsedArgs.ContainsKey("-sync"))
                sync = true;

            if (parsedArgs.ContainsKey("-verbose"))
                verbose = true;
        }

        private void banner()
        {
            System.Console.WriteLine("Receiving {0} messages on subject {1}",
                count, subject);
            System.Console.WriteLine("  Url: {0}", url);
            System.Console.WriteLine("  Receiving: {0}",
                sync ? "Synchronously" : "Asynchronously");
        }

        public static void Main(string[] args)
        {
            try
            {
                new Replier().Run(args);
            }
            catch (Exception ex)
            {
                System.Console.Error.WriteLine("Exception: " + ex.Message);
                System.Console.Error.WriteLine(ex);
            }
        }
    }
}
