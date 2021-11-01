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
    /// <summary>
    /// This example will demonstrate JetStream publishing with options.
    /// </summary>
    internal static class JetStreamPublishWithOptionsUseCases
    {
        private const string Usage =
            "Usage: JetStreamPublishWithOptionsUseCases [-url url] [-creds file] [-stream stream] " +
            "[-subject subject]" +
            "\n\nDefault Values:" +
            "\n   [-stream]   pubopts-stream" +
            "\n   [-subject]  pubopts-subject";

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("JetStream Publish With Options Use Cases", args, Usage)
                .DefaultStream("pubopts-stream")
                .DefaultSubject("pubopts-subject")
                .Build();

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Create a JetStreamManagement context.
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                    
                    // Use the utility to create a stream stored in memory.
                    JsUtils.CreateStreamExitWhenExists(jsm, helper.Stream, helper.Subject);

                    // get a regular context
                    IJetStream js = c.CreateJetStreamContext();

                    PublishOptions.PublishOptionsBuilder builder = PublishOptions.Builder()
                        .WithExpectedStream(helper.Stream)
                        .WithMessageId("mid1");

                    PublishAck pa = js.Publish(helper.Subject, Encoding.ASCII.GetBytes("message1"), builder.Build());
                    Console.WriteLine("Published message on subject {0}, stream {1}, seqno {2}.",
                        helper.Subject, pa.Stream, pa.Seq);
                    
                    // IMPORTANT!
                    // You can reuse the builder in 2 ways.
                    // 1. Manually set a field to null or to DefaultLastSequence if you want to clear it out.
                    // 2. Use the clearExpected method to clear the expectedLastId, expectedLastSequence and messageId fields

                    // Manual re-use 1. Clearing some fields
                    builder
                        .WithExpectedLastMsgId("mid1")
                        .WithExpectedLastSequence(PublishOptions.DefaultLastSequence)
                        .WithMessageId(null);
                    pa = js.Publish(helper.Subject, Encoding.ASCII.GetBytes("message2"), builder.Build());
                    Console.WriteLine("Published message on subject {0}, stream {1}, seqno {2}.",
                        helper.Subject, pa.Stream, pa.Seq);

                    // Manual re-use 2. Setting all the expected fields again
                    builder
                        .WithExpectedLastMsgId(null)
                        .WithExpectedLastSequence(PublishOptions.DefaultLastSequence)
                        .WithMessageId("mid3");
                    pa = js.Publish(helper.Subject, Encoding.ASCII.GetBytes("message3"), builder.Build());
                    Console.WriteLine("Published message on subject {0}, stream {1}, seqno {2}.",
                        helper.Subject, pa.Stream, pa.Seq);

                    // reuse() method clears all the fields, then we set some fields.
                    builder.ClearExpected()
                        .WithExpectedLastSequence(pa.Seq)
                        .WithMessageId("mid4");
                    pa = js.Publish(helper.Subject, Encoding.ASCII.GetBytes("message4"), builder.Build());
                    Console.WriteLine("Published message on subject {0}, stream {1}, seqno {2}.",
                        helper.Subject, pa.Stream, pa.Seq);

                    // exception when the expected stream does not match [10060]
                    try
                    {
                        PublishOptions errOpts = PublishOptions.Builder().WithExpectedStream("wrongStream").Build();
                        js.Publish(helper.Subject, Encoding.ASCII.GetBytes("ex1"), errOpts);
                    }
                    catch (NATSJetStreamException e)
                    {
                        Console.WriteLine($"Exception was: '{e.ErrorDescription}'");
                    }

                    // exception with wrong last msg ID [10070]
                    try
                    {
                        PublishOptions errOpts = PublishOptions.Builder().WithExpectedLastMsgId("wrongId").Build();
                        js.Publish(helper.Subject, Encoding.ASCII.GetBytes("ex2"), errOpts);
                    }
                    catch (NATSJetStreamException e)
                    {
                        Console.WriteLine($"Exception was: '{e.ErrorDescription}'");
                    }

                    // exception with wrong last sequence wrong last sequence: 4 [10071]
                    try
                    {
                        PublishOptions errOpts = PublishOptions.Builder().WithExpectedLastSequence(999).Build();
                        js.Publish(helper.Subject, Encoding.ASCII.GetBytes("ex3"), errOpts);
                    }
                    catch (NATSJetStreamException e)
                    {
                        Console.WriteLine($"Exception was: '{e.ErrorDescription}'");
                    }

                    // delete the stream since we are done with it.
                    jsm.DeleteStream(helper.Stream);
                }
            }
            catch (Exception ex)
            {
                helper.ReportException(ex);
            }
        }
    }
}