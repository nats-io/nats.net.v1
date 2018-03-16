// Copyright 2016-2018 The NATS Authors
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
using System.Linq;
using NATS.Client;
using System.Threading;
using System.Security.Cryptography.X509Certificates;
using System.Diagnostics;

/// <summary>
/// This is a very long running reconnection test, to thouroughly flush out
/// reconnection related issues as servers in a cluster come and go.
/// </summary>
namespace ReconnectTestClient
{
    class ReconnectTest
    {
        Dictionary<string, string> parsedArgs = new Dictionary<string, string>();

        int count = 1000000;
        static int CLIENT_COUNT = 20;
        string url = Defaults.Url;
        string tlsKey = "certs\\client.pfx";
        string password = "password";
        string subject = "foo";
        List<Client> clients = new List<Client>();

        public class Server : IDisposable
        {
            Process p;
            ProcessStartInfo psInfo;
            string args = null;

            public Server(string args)
            {
                this.args = args;
            }

            public void Start()
            {
                psInfo = new ProcessStartInfo("gnatsd.exe", args);
                psInfo.WorkingDirectory = Environment.CurrentDirectory;
                p = Process.Start(psInfo);
            }

            public void Shutdown()
            {
                if (p == null)
                    return;

                try
                {
                    p.Kill();
                }
                catch (Exception) { }

                p = null;
            }

            void IDisposable.Dispose()
            {
                Shutdown();
            }
        }

        // Launch servers to start and fail one by one.
        // This tests basic reconnect logic, but also
        // cascading failures withing the reconnect process
        // itself.
        public static void RunServers()
        {
            Thread t = new Thread(() =>
            {
                Server s;
                List<Server> l = new List<Server>();

                l.Add(new Server(" -D -config s1.conf"));
                l.Add(new Server(" -D -config s2.conf"));
                l.Add(new Server(" -D -config s3.conf"));

                l[0].Start();
                l[1].Start();
                l[2].Start();

                Thread.Sleep(2000);

                while (true)
                {
                    for (int i = 0; i < l.Count; i++)
                    {
                        // pop the first from the list and move
                        // to the end.
                        s = l.First();
                        l.Remove(s);
                        l.Add(s);

                        // bounce the server.
                        s.Shutdown();
                        Thread.Sleep(500);
                        s.Start();
                    }

                    Thread.Sleep(2000);
                }
            });
            t.Name = "ServerCycler";
            t.Start();
        }

        public static void RunServersBounceAllDelay()
        {
            Thread t = new Thread(() =>
            {
                List<Server> l = new List<Server>();

                l.Add(new Server(" -D -config s1.conf"));
                l.Add(new Server(" -D -config s2.conf"));
                l.Add(new Server(" -D -config s3.conf"));

                l[0].Start();
                l[1].Start();
                l[2].Start();

                while (true)
                {
                    Thread.Sleep(5000);

                    foreach (Server srv in l)
                    {
                        srv.Shutdown();
                    }

                    // allow a bunch of reconnect
                    int downtime = 15000;

                    Console.WriteLine("Stopped servers");
                    int remaining = downtime / 1000;
                    for (int i = 0; i < (downtime/ 5000); i++)
                    {
                        
                        Console.WriteLine("Starting servers in {0} seconds...", remaining);
                        remaining -= 5;
                        Thread.Sleep(5000);
                    }

                    foreach (Server srv in l)
                    {
                        srv.Start();
                    }
                    Console.WriteLine("Started servers.");
                }
            });
            t.Name = "ServerCycler";
            t.Start();
        }

        public class Client
        {
            Object clientLock = new Object();

            string subject = "foo";
            string tlsKey = "client.pfx";
            public string clientName = null;
            string password = "password";
            int reconnMissedCount = 0;

            private static bool rcvcb(object sender, X509Certificate certificate, X509Chain chain, System.Net.Security.SslPolicyErrors sslPolicyErrors)
            {
                return true;
            }

            public static Options CreateClientOptions(string clientName, string tlsKey, string tlsPassword)
            {
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Servers = new string[] { 
                    "nats://natsuser:natspassword@127.0.0.1:4000", 
                    "nats://natsuser:natspassword@127.0.0.1:5000", 
                    "nats://natsuser:natspassword@127.0.0.1:6000" 
                };

                opts.Name = clientName;
                opts.Secure = true;

                X509Certificate2 cert = new X509Certificate2(tlsKey, tlsPassword);
                opts.TLSRemoteCertificationValidationCallback += rcvcb;

                opts.AddCertificate(cert);
                opts.MaxReconnect = 3000;
                opts.ReconnectWait = 1500;

                opts.ClosedEventHandler += (sender, args) =>
                {
                    Console.WriteLine("Client {0} CLOSED!.", clientName);
                };

                return opts;
            }

            public bool Reconnected
            {
                set
                {
                    lock (clientLock)
                    {
                        if (value == false)
                        {
                            reconnMissedCount++;
                        }
                        else
                        {
                            reconnMissedCount = 0;
                        }
                    }
                }
                get
                {
                    lock (clientLock)
                    {
                        return reconnMissedCount == 0;
                    }

                }
            }

            public int CurrentMissedReconnects
            {
                get
                {
                    lock (clientLock)
                    {
                        return reconnMissedCount;
                    }
                }
            }

            public Client(string clientName, string tlsKey, string password, string subject)
            {
                this.clientName = clientName;
                this.tlsKey = tlsKey;
                this.subject = subject;
            }

            public void Start()
            {
                Thread t = new Thread(Run);
                t.Name = clientName;
                t.Start();
            }

            public void Run()
            {
                Options opts = CreateClientOptions(clientName, tlsKey, password);
                opts.ReconnectedEventHandler += (sender, args) =>
                {
                    Console.WriteLine("Client {0} reconnected.", clientName);
                    Reconnected = true;
                };

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    Console.WriteLine("Client {0} connected.", clientName);

                    EventHandler<MsgHandlerEventArgs> msgHandler = (sender, cbArgs) =>
                    {
                        //NOOP
                    };

                    using (IAsyncSubscription s = c.SubscribeAsync(subject, msgHandler))
                    {
                        // Go ahead and keep a thread up for stressing the system a bit.
                        while (true)
                        {
                            // always up
                            Thread.Sleep(250);
                        }
                    }
                }
            }
        }
        
        public void Run(string[] args)
        {
            List<Client> clients = new List<Client>();

            for (int i = 0; i < CLIENT_COUNT; i++)
            {
                Client c = new Client(("SRTClient:" + i), tlsKey, password, subject);
                c.Start();
                clients.Add(c);
            }

            Thread t = new Thread(() =>
            {
                while (true)
                {
                    Thread.Sleep(10000);

                    int badClientCount = 0;

                    for (int i = 0; i < clients.Count; i++)
                    {
                        if (clients[i].Reconnected == false)
                        {
                            Console.WriteLine("{0:HH:mm:ss} Client {1} may be locked with {2} missed attempts.",
                                        DateTime.Now, clients[i].clientName, clients[i].CurrentMissedReconnects);
                            badClientCount++;
                        }
                        clients[i].Reconnected = false;
                    }

                    Console.WriteLine("{0:HH:mm:ss} {1} clients checked, {2} are unhealthy.",
                        DateTime.Now, clients.Count, badClientCount);
                }
            });
            t.Name = "HealthChecker";
            t.Start();
        }

        private void usage()
        {
            Console.Error.WriteLine(
                "Usage:  Subscribe [-url url] [-subject subject] " +
                "-tlsKey key [-verbose]");

            System.Environment.Exit(-1);
        }

        private void parseArgs(string[] args)
        {
            if (args == null)
                return;

            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("-verbose"))
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

            if (parsedArgs.ContainsKey("-tlskey"))
                tlsKey = parsedArgs["-tlsKey"];

            if (parsedArgs.ContainsKey("-password"))
                tlsKey = parsedArgs["-password"];
            
        }

        private void banner()
        {
            Console.WriteLine("Receiving {0} messages on subject {1}",
                count, subject);
            Console.WriteLine("  Url: {0}", url);
        }

        public static void Main(string[] args)
        {
            System.Threading.ThreadPool.SetMinThreads(300, 300);

            try
            {
                RunServers();
                // Alternatively, one can bounce all servers 
                // with a delay.
                Thread.Sleep(1000);
                new ReconnectTest().Run(args);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception: " + ex.Message);
                Console.Error.WriteLine(ex);
            }
        }
    } 
}
