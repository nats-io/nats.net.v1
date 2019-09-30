// Copyright 2015-2018 The NATS Authors
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

using System;
using System.Diagnostics;
using System.Text;
using System.Collections.Generic;
using NATS.Client;


namespace NATSExamples
{
    class Publisher
    {
        Dictionary<string, string> parsedArgs = new Dictionary<string, string>();

        int count = 1;
        string url = Defaults.Url;
        string subject = "foo";
        byte[] payload = null;
        string creds = null;

        public void Run(string[] args)
        {
            Stopwatch sw = null;

            parseArgs(args);
            banner();

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = url;
            if (creds != null)
            {
                opts.SetUserCredentials(creds);
            }

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                sw = Stopwatch.StartNew();

                for (int i = 0; i < count; i++)
                {
                    c.Publish(subject, payload);
                }
                c.Flush();

                sw.Stop();

                Console.Write("Published {0} msgs in {1} seconds ", count, sw.Elapsed.TotalSeconds);
                Console.WriteLine("({0} msgs/second).",
                    (int)(count / sw.Elapsed.TotalSeconds));
                printStats(c);

            }
        }

        private void printStats(IConnection c)
        {
            IStatistics s = c.Stats;
            Console.WriteLine("Statistics:  ");
            Console.WriteLine("   Outgoing Payload Bytes: {0}", s.OutBytes);
            Console.WriteLine("   Outgoing Messages: {0}", s.OutMsgs);
        }

        private void usage()
        {
            Console.Error.WriteLine(
                "Usage:  Publish [-url url] [-subject subject] " +
                "-count [count] -creds [file] [-payload payload]");

            Environment.Exit(-1);
        }

        private void parseArgs(string[] args)
        {
            if (args == null)
                return;

            for (int i = 0; i < args.Length; i++)
            {
                if (i + 1 == args.Length)
                    usage();

                parsedArgs.Add(args[i], args[i + 1]);
                i++;
            }

            if (parsedArgs.ContainsKey("-count"))
                count = Convert.ToInt32(parsedArgs["-count"]);

            if (parsedArgs.ContainsKey("-url"))
                url = parsedArgs["-url"];

            if (parsedArgs.ContainsKey("-subject"))
                subject = parsedArgs["-subject"];

            if (parsedArgs.ContainsKey("-payload"))
                payload = Encoding.UTF8.GetBytes(parsedArgs["-payload"]);

            if (parsedArgs.ContainsKey("-creds"))
                creds = parsedArgs["-creds"];
        }

        private void banner()
        {
            Console.WriteLine("Publishing {0} messages on subject {1}",
                count, subject);
            Console.WriteLine("  Url: {0}", url);
            Console.WriteLine("  Subject: {0}", subject);
            Console.WriteLine("  Count: {0}", count);
            Console.WriteLine("  Payload is {0} bytes.",
                payload != null ? payload.Length : 0);
        }

        public static void Main(string[] args)
        {
            try
            {
                new Publisher().Run(args);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception: " + ex.Message);
                Console.Error.WriteLine(ex);
            }

        }
    }
}
