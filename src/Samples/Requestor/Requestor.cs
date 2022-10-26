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
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using NATS.Client;

namespace NATSExamples
{
    /// <summary>
    /// Requestor makes requests which the Replier will respond to.
    /// The Replier must be started before the Requestor is run.
    /// You can also try without the Replier running to see "No responders".
    /// </summary>
    class Requestor
    {
        Dictionary<string, string> parsedArgs = new Dictionary<string, string>();

        int count = 10;
        string url = Defaults.Url;
        string subject = "RequestReply";
        string creds;

        public void Run(string[] args)
        {
            ParseArgs(args);
            Banner();

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = url;
            if (creds != null)
            {
                opts.SetUserCredentials(creds);
            }

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                Stopwatch sw = Stopwatch.StartNew();

                for (int i = 1; i <= count; i++)
                {
                    string payload = $"Request #{i}";
                    Console.WriteLine($"\r\nRequesting message with subject \"{subject}\" and payload \"{payload}\".");
                    Msg reply = c.Request(subject, Encoding.ASCII.GetBytes(payload));
                    Console.WriteLine($"Received \"{Encoding.ASCII.GetString(reply.Data)}\" as reply for payload \"{payload}\".");
                }
                c.Flush();

                sw.Stop();

                Console.Write("Completed {0} requests in {1} seconds ", count, sw.Elapsed.TotalSeconds);
                Console.WriteLine("({0} requests/second).", (int)(count / sw.Elapsed.TotalSeconds));
                PrintStats(c);
            }
        }

        private void PrintStats(IConnection c)
        {
            IStatistics s = c.Stats;
            Console.WriteLine("Statistics:  ");
            Console.WriteLine("   Incoming Payload Bytes: {0}", s.InBytes);
            Console.WriteLine("   Incoming Messages: {0}", s.InMsgs);
            Console.WriteLine("   Outgoing Payload Bytes: {0}", s.OutBytes);
            Console.WriteLine("   Outgoing Messages: {0}", s.OutMsgs);
        }

        private void Usage()
        {
            Console.Error.WriteLine(
                "Usage:  Requestor [-url url] [-subject subject] " +
                "[-count count] [-creds file] [-payload payload]");

            Environment.Exit(-1);
        }

        private void ParseArgs(string[] args)
        {
            if (args == null)
                return;

            for (int i = 0; i < args.Length; i++)
            {
                if (i + 1 == args.Length)
                    Usage();

                parsedArgs.Add(args[i], args[i + 1]);
                i++;
            }

            if (parsedArgs.ContainsKey("-count"))
                count = Convert.ToInt32(parsedArgs["-count"]);

            if (parsedArgs.ContainsKey("-url"))
                url = parsedArgs["-url"];

            if (parsedArgs.ContainsKey("-subject"))
                subject = parsedArgs["-subject"];

            if (parsedArgs.ContainsKey("-creds"))
                creds = parsedArgs["-creds"];
        }

        private void Banner()
        {
            Console.WriteLine("Sending {0} requests on subject {1}", count, subject);
            Console.WriteLine("  Url: {0}", url);
        }

        public static void Main(string[] args)
        {
            try
            {
                new Requestor().Run(args);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception: " + ex.Message);
                Console.Error.WriteLine(ex);
            }

        }
    }
}
