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
    class JetStreamPublish
    {
        const string Usage = "Usage: JetStreamPublish [-url url] [-creds file] [-stream stream] " +
                                   "[-subject subject] [-count count] [-payload payload] [-header key:value]" +
                                   "\n\nDefault Values:" +
                                   "\n   [-stream]   example-stream" +
                                   "\n   [-subject]  example-subject" +
                                   "\n   [-count]    10" +
                                   "\n   [-payload]  Hello" +
                                   "\n\nRun Notes:" +
                                   "\n   - count < 1 is the same as 1" +
                                   "\n   - quote multi word payload" +
                                   "\n   - headers are optional, quote multi word value, no ':' in value please";
        
        string url = Defaults.Url;
        string creds;
        string stream = "example-stream";
        string subject = "example-subject";
        string payload = "Hello";
        MsgHeader header = new MsgHeader();
        int count = 10;

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
                CreateStreamWhenDoesNotExist(c);

                IJetStream js = c.CreateJetStreamContext();

                byte[] data = Encoding.UTF8.GetBytes(payload);

                int stop = count < 2 ? 2 : count + 1;
                for (int x = 1; x < stop; x++)
                {
                    // make unique message data if you want more than 1 message
                    if (count > 1)
                    {
                        data = Encoding.UTF8.GetBytes(payload + "-" + x);
                    }

                    // Publish a message and print the results of the publish acknowledgement.
                    Msg msg = new Msg(subject, null, header, data);

                    // We'll use the defaults for this simple example, but there are options
                    // to constrain publishing to certain streams, expect sequence numbers and
                    // more. See the JetStreamPublishWithOptionsUseCases example for details.
                    // An exception will be thrown if there is a failure.
                    PublishAck pa = js.Publish(msg);
                    Console.WriteLine("Published message {0} on subject {1}, stream {2}, seqno {3}.",
                        Encoding.UTF8.GetString(data), subject, pa.Stream, pa.Seq);
                }
            }
        }

        void CreateStreamWhenDoesNotExist(IConnection c)
        {
            IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

            try
            {
                jsm.GetStreamInfo(stream); // this throws if the stream does not exist
                return;
            }
            catch (NATSJetStreamException) { /* stream does not exist */ }
            
            StreamConfiguration sc = StreamConfiguration.Builder()
                .WithName(stream)
                .WithStorageType(StorageType.Memory)
                .WithSubjects(subject)
                .Build();
            jsm.AddStream(sc);
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

                    case "-payload": 
                        payload = args[i + 1];
                        break;

                    case "-count": 
                        count = Convert.ToInt32(args[i + 1]);
                        break;

                    case "-header": 
                        string[] split = args[i + 1].Split(':');
                        if (split.Length != 2) { UsageThenExit(); }
                        header.Add(split[0], split[1]);
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
            Console.WriteLine("  Count: {0}", count);
            Console.WriteLine("  Payload is \"{0}\"", payload);
            Console.WriteLine("  Headers: {0}", header.Count == 0 ? "No" : "Yes");
        }

        public static void Main(string[] args)
        {
            try
            {
                new JetStreamPublish().Run(args);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception: " + ex.Message);
                Console.Error.WriteLine(ex);
            }
        }
    }
}