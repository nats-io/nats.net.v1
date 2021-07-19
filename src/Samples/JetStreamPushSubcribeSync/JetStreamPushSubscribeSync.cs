// Copyright 2021 The NATS Authors
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
using System.Text;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    public class JetStreamPushSubscribeSync
    {
        const string Usage = "Usage: JsPub [-url url] [-creds file] [-stream stream] " +
                                   "[-subject subject] [-count count] [-durable durable] [-deliver deliverSubject]" +
                                   "\n\nDefault Values:" +
                                   "\n   [-stream]   example-stream" +
                                   "\n   [-subject]  example-subject" +
                                   "\n   [-count]    0" +
                                   "\n\nRun Notes:" +
                                   "\n   - durable is optional, durable behaves differently, try it by running this twice with durable set" +
                                   "\n   - deliver is optional" + 
                                   "\n   - msg_count < 1 will just loop until there are no more messages";
        
        string url = Defaults.Url;
        string creds;
        string stream = "example-stream";
        string subject = "example-subject";
        string durable;
        string deliver;
        int count = 0;

        void Run(string[] args)
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
                StreamMustExist(c);

                IJetStream js = c.CreateJetStreamContext();

                PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                    .WithStream(stream) // saves a lookup
                    .WithDurable(durable)
                    .WithDeliverSubject(deliver)
                    .Build();

                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(subject, pso);
                c.Flush(500); // flush outgoing communication with/to the server

                int red = 0;
                Msg msg = GetNextMessage(sub);
                while (msg != null) {
                    Console.WriteLine("\nMessage Received:");
                    if (msg.HasHeaders) {
                        Console.WriteLine("  Headers:");
                        foreach (string key in msg.Header.Keys) {
                            Console.WriteLine("    {0}: {1}", key, msg.Header[key]);
                        }
                    }

                    Console.WriteLine("  Subject: {0}\n  Data: {1}", msg.Subject, Encoding.UTF8.GetString(msg.Data));

                    // This check may not be necessary for this example depending
                    // on how the consumer has been setup.  When a deliver subject
                    // is set on a consumer, messages can be received from applications
                    // that are NATS producers and from streams in NATS servers.
                    if (msg.IsJetStream) {
                        Console.WriteLine("  Meta: " + msg.MetaData);
                        // Because this is a synchronous subscriber, there's no auto-ack.
                        // We need to ack the message or it'll be redelivered.  
                        msg.Ack();
                    }

                    ++red;
                    if (--count == 0) {
                        msg = null;
                    }
                    else {
                        msg = GetNextMessage(sub);
                    }
                }

                Console.WriteLine("\n" + red + " message(s) were received.\n");

                sub.Unsubscribe();
            }
        }

        private static Msg GetNextMessage(IJetStreamPushSyncSubscription sub)
        {
            try
            {
                return sub.NextMessage(500);
            }
            catch (NATSTimeoutException)
            {
                // probably no messages in stream
                return null;
            }
        }

        void StreamMustExist(IConnection c)
        {
            try
            {
                c.CreateJetStreamManagementContext().GetStreamInfo(stream); // this throws if the stream does not exist
            }
            catch (NATSJetStreamException)
            {
                Console.Error.WriteLine("Stream Must Already Exist!");
                Environment.Exit(-1);
            }
        }
        
        void UsageThenExit()
        {
            Console.Error.WriteLine(Usage);
            Environment.Exit(-1);
        }
        
        void ParseArgs(string[] args)
        {
            if (args == null) { return; } // will just use defaults
            if (args.Length % 2 == 1) { UsageThenExit(); } // odd number of arguments
            
            for (int i = 0; i < args.Length; i += 2)
            {
                switch (args[i])
                {
                    case "-url":
                        url = args[i + 1];
                        break;

                    case "-creds": 
                        creds = args[i + 1];
                        break;

                    case "-stream": 
                        subject = args[i + 1];
                        break;

                    case "-subject": 
                        subject = args[i + 1];
                        break;

                    case "-durable": 
                        durable = args[i + 1];
                        break;

                    case "-deliver": 
                        deliver = args[i + 1];
                        break;
                }
            }
        }

        void Banner()
        {
            Console.WriteLine("JetStream Publishing Example");
            Console.WriteLine("  Url: {0}", url);
            Console.WriteLine("  Stream: {0}", stream);
            Console.WriteLine("  Subject: {0}", subject);
            Console.WriteLine("  Durable: {0}", durable ?? "N/A");
            Console.WriteLine("  Deliver: {0}", deliver ?? "N/A");
            Console.WriteLine("  Count: {0}", count);
            Console.WriteLine();
        }

        public static void Main(string[] args)
        {
            try
            {
                new JetStreamPushSubscribeSync().Run(args);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception: " + ex.Message);
                Console.Error.WriteLine(ex);
            }
        }
       
    }
}