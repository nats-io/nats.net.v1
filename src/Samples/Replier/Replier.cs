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
using System.Threading;
using NATS.Client;

namespace NATSExamples
{
    class Replier
    {
        Dictionary<string, string> parsedArgs = new Dictionary<string, string>();

        int count = 10;
        string url = Defaults.Url;
        string subject = "RequestReply";
        bool sync = false;
        string creds = null;
        int received = 0;

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
                TimeSpan elapsed;

                if (sync)
                {
                    elapsed = ReceiveSyncSubscriber(c);
                }
                else
                {
                    elapsed = ReceiveAsyncSubscriber(c);
                }

                Console.Write("Replied to {0} msgs in {1} seconds ", received, elapsed.TotalSeconds);
                Console.WriteLine("({0} replies/second).", (int)(received / elapsed.TotalSeconds));
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

        private void ProcessRequest(IConnection c, Msg msg)
        {
            string msgSubject = msg.Subject;
            string msgPayload = Encoding.ASCII.GetString(msg.Data);
            Console.WriteLine($"\r\nReceived message with subject \"{msgSubject}\" and payload \"{msgPayload}\".");
            c.Publish(new Msg(msg.Reply, Encoding.ASCII.GetBytes("Replying to " + msgPayload)));
            c.Flush();
        }

        private TimeSpan ReceiveAsyncSubscriber(IConnection c)
        {
            Stopwatch sw = new Stopwatch();
            AutoResetEvent subDone = new AutoResetEvent(false);

            void MsgHandler(object sender, MsgHandlerEventArgs args)
            {
                if (received == 0)
                {
                    sw.Start();
                }

                ProcessRequest(c, args.Message);
                if (++received == count)
                {
                    sw.Stop();
                    subDone.Set();
                }
            }

            using (IAsyncSubscription s = c.SubscribeAsync(subject, MsgHandler))
            {
                // just wait to complete
                subDone.WaitOne();
            }
            return sw.Elapsed;
        }
        
        private TimeSpan ReceiveSyncSubscriber(IConnection c)
        {
            using (ISyncSubscription s = c.SubscribeSync(subject))
            {
                Stopwatch sw = new Stopwatch();
                while (received < count)
                {
                    if (received == 0)
                    {
                        sw.Start();
                    }
                    ProcessRequest(c, s.NextMessage());
                    received++; 
                }
                sw.Stop();
                return sw.Elapsed;
            }
        }

        private void Usage()
        {
            Console.Error.WriteLine(
                "Usage:  Replier [-url url] [-subject subject] " +
                "[-count count] [-creds file] [-sync] [-verbose]");

            Environment.Exit(-1);
        }

        private void ParseArgs(string[] args)
        {
            if (args == null)
                return;

            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("-sync"))
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

            if (parsedArgs.ContainsKey("-sync"))
                sync = true;

            if (parsedArgs.ContainsKey("-creds"))
                creds = parsedArgs["-creds"];
        }

        private void Banner()
        {
            Console.WriteLine("Receiving {0} messages on subject {1}", count, subject);
            Console.WriteLine("  Url: {0}", url);
            Console.WriteLine("  Receiving: {0}", sync ? "Synchronously" : "Asynchronously");
        }

        public static void Main(string[] args)
        {
            try
            {
                new Replier().Run(args);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception: " + ex.Message);
                Console.Error.WriteLine(ex);
            }
        }
    }
}
