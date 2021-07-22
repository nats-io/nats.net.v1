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
    class JetStreamPublishWithOptionsUseCases
    {
        private const string Usage = "Usage: JetStreamPublishVsCorePublish [-url url] [-creds file] [-stream stream] " +
                                     "[-subject subject]" +
                                     "\n\nDefault Values:" +
                                     "\n   [-stream]   pubopts-stream" +
                                     "\n   [-subject]  pubopts-subject";
        
        string url = Defaults.Url;
        string creds;
        string stream = "pubopts-stream";
        string subject = "pubopts-subject";

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

                PublishOptions.PublishOptionsBuilder builder = PublishOptions.Builder()
                    .WithExpectedStream(stream)
                    .WithMessageId("mid1");

                PublishAck pa = js.Publish(subject, Encoding.ASCII.GetBytes("message1"), builder.Build());
                Console.WriteLine("Published message on subject {0}, stream {1}, seqno {2}.",
                    subject, pa.Stream, pa.Seq);
                

                // IMPORTANT!
                // You can reuse the builder in 2 ways.
                // 1. Manually set a field to null or to DefaultLastSequence if you want to clear it out.
                // 2. Use the clearExpected method to clear the expectedLastId, expectedLastSequence and messageId fields

                // Manual re-use 1. Clearing some fields
                builder
                    .WithExpectedLastMsgId("mid1")
                    .WithExpectedLastSequence(PublishOptions.DefaultLastSequence)
                    .WithMessageId(null);
                pa = js.Publish(subject, Encoding.ASCII.GetBytes("message2"), builder.Build());
                Console.WriteLine("Published message on subject {0}, stream {1}, seqno {2}.",
                    subject, pa.Stream, pa.Seq);                

                // Manual re-use 2. Setting all the expected fields again
                builder
                    .WithExpectedLastMsgId(null)
                    .WithExpectedLastSequence(PublishOptions.DefaultLastSequence)
                    .WithMessageId("mid3");
                pa = js.Publish(subject, Encoding.ASCII.GetBytes("message3"), builder.Build());
                Console.WriteLine("Published message on subject {0}, stream {1}, seqno {2}.",
                    subject, pa.Stream, pa.Seq);                

                // reuse() method clears all the fields, then we set some fields.
                builder.ClearExpected()
                    .WithExpectedLastSequence(pa.Seq)
                    .WithMessageId("mid4");
                pa = js.Publish(subject, Encoding.ASCII.GetBytes("message4"), builder.Build());
                Console.WriteLine("Published message on subject {0}, stream {1}, seqno {2}.",
                    subject, pa.Stream, pa.Seq);                

                // exception when the expected stream does not match
                try {
                    PublishOptions errOpts = PublishOptions.Builder().WithExpectedStream("wrongStream").Build();
                    js.Publish(subject, Encoding.ASCII.GetBytes("ex1"), errOpts);
                } catch (NATSJetStreamException e) {
                    Console.WriteLine(e.ErrorDescription);
                }

                // exception with wrong last msg ID
                try {
                    PublishOptions errOpts = PublishOptions.Builder().WithExpectedLastMsgId("wrongId").Build();
                    js.Publish(subject, Encoding.ASCII.GetBytes("ex2"), errOpts);
                } catch (NATSJetStreamException e) {
                    Console.WriteLine(e.ErrorDescription);
                }

                // exception with wrong last sequence
                try {
                    PublishOptions errOpts = PublishOptions.Builder().WithExpectedLastSequence(999).Build();
                    js.Publish(subject, Encoding.ASCII.GetBytes("ex3"), errOpts);
                } catch (NATSJetStreamException e) {
                    Console.WriteLine(e.ErrorDescription);
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
                new JetStreamPublishWithOptionsUseCases().Run(args);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception: " + ex.Message);
                Console.Error.WriteLine(ex);
            }
        }
    }
}