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
    class JetStreamPublishVsCorePublish
    {
        private const string Usage = "Usage: JetStreamPublishVsCorePublish [-url url] [-creds file] [-stream stream] " +
                                     "[-subject subject]" +
                                     "\n\nDefault Values:" +
                                     "\n   [-stream]   js-vs-reg-stream" +
                                     "\n   [-subject]  js-vs-reg-subject";
        
        string url = Defaults.Url;
        string creds;
        string stream = "js-vs-reg-stream";
        string subject = "js-vs-reg-subject";

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

                // Regular Nats publish is straightforward
                c.Publish(subject, Encoding.ASCII.GetBytes("regular-message"));

                // A JetStream publish allows you to set publish options
                // that a regular publish does not.
                // A JetStream publish returns an ack of the publish. There
                // is no ack in a regular message.
                Msg msg = new Msg(subject, Encoding.ASCII.GetBytes("js-message"));

                PublishOptions po = PublishOptions.Builder()
                    // .WithExpectedLastSequence(...)
                    // .WithExpectedLastMsgId(...)
                    // .WithExpectedStream(...)
                    // .WithMessageId()
                    .Build();

                PublishAck pa = js.Publish(msg, po);
                Console.WriteLine(pa);

                // set up the subscription
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(subject);
                c.Flush(500); // flush outgoing communication with/to the server
                
                // Both messages appear in the stream
                msg = sub.NextMessage(500);
                msg.Ack();
                Console.WriteLine("Received Data: " + Encoding.ASCII.GetString(msg.Data) + "\n         Meta: " + msg.MetaData);

                msg = sub.NextMessage(500);
                msg.Ack();
                Console.WriteLine("Received Data: " + Encoding.ASCII.GetString(msg.Data) + "\n         Meta: " + msg.MetaData);
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
                }
            }
        }

        void Banner()
        {
            Console.WriteLine("JetStream Publishing Versus Core Publishing Example");
            Console.WriteLine("  Url: {0}", url);
            Console.WriteLine("  Stream: {0}", stream);
            Console.WriteLine("  Subject: {0}", subject);
        }

        public static void Main(string[] args)
        {
            try
            {
                new JetStreamPublishVsCorePublish().Run(args);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception: " + ex.Message);
                Console.Error.WriteLine(ex);
            }
        }
    }
}