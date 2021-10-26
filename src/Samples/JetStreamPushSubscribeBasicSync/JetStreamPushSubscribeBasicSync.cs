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
    /// This example will demonstrate JetStream basic synchronous push subscribing.
    /// Run NatsJsPub first to setup message data.
    /// </summary>
    internal static class JetStreamPushSubscribeBasicSync
    {
        private const string Usage =
            "Usage: JetStreamPushSubscribeBasicSync [-url url] [-creds file] [-stream stream] " +
            "[-subject subject] [-count count] [-durable durable]" +
            "\n\nDefault Values:" +
            "\n   [-stream]  example-stream" +
            "\n   [-subject] example-subject" +
            "\n   [-count]   0" +
            "\n\nRun Notes:" +
            "\n   - make sure you have created and published to the stream and subject, maybe using the JetStreamPublish example" +
            "\n   - durable is optional, durable behaves differently, try it by running this twice with durable set" +
            "\n   - msg_count < 1 will just loop until there are no more messages";
        
        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("Push Subscribe Basic Sync", args, Usage)
                .DefaultStream("example-stream")
                .DefaultSubject("example-subject")
                .DefaultCount(0, true) // true indicated 0 means unlimited
                // .DefaultDurable("push-sub-basic-sync-durable")
                .Build();

            int count = helper.Count < 1 ? int.MaxValue : helper.Count;

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // The stream (and data) must exist
                    JsUtils.ExitIfStreamNotExists(c, helper.Stream);

                    // Create our JetStream context.
                    IJetStream js = c.CreateJetStreamContext();

                    // Build our subscription options.
                    // * A push subscription means the server will "push" us messages.
                    // * Durable means the server will remember where we are if we use that name.
                    // * Durable can by null or empty, the builder treats them the same.
                    // * The stream name is not technically required. If it is not provided, the
                    //   code building the subscription will look it up by making a request to the server.
                    //   If you know the stream name, you might as well supply it and save a trip to the server.
                    PushSubscribeOptions so = PushSubscribeOptions.Builder()
                            .WithStream(helper.Stream)
                            .WithDurable(helper.Durable) // it's okay if this is null, the builder handles it
                            .Build();

                    // Subscribe synchronously, then just wait for messages.
                    IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(helper.Subject, so);
                    c.Flush(5000);

                    int red = 0;
                    while (count > 0) {
                        try
                        {
                            Msg msg = sub.NextMessage(1000);
                            Console.WriteLine("\nMessage Received:");
                            if (msg.HasHeaders) {
                                Console.WriteLine("  Headers:");
                                foreach (string key in msg.Header.Keys) {
                                    foreach (string value in msg.Header.GetValues(key)) {
                                        Console.WriteLine($"    {key}: {value}");
                                    }
                                }
                            }

                            Console.WriteLine("  Subject: {0}\n  Data: {1}\n", msg.Subject, Encoding.UTF8.GetString(msg.Data));
                            Console.WriteLine("  " + msg.MetaData);
                            
                            // Because this is a synchronous subscriber, there's no auto-ack.
                            // The default Consumer Configuration AckPolicy is Explicit
                            // so we need to ack the message or it'll be redelivered.
                            msg.Ack();

                            ++red;
                            --count;
                        }
                        catch (NATSTimeoutException) // timeout means there are no messages available
                        {
                            count = 0; // ran out of messages
                        }
                    }

                    Console.WriteLine("\n" + red + " message(s) were received.\n");

                    sub.Unsubscribe();
                    c.Flush(5000);
                }
            }
            catch (Exception ex)
            {
                helper.ReportException(ex);
            }
        }
    }
}