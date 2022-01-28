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
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    /// <summary>
    /// This example will demonstrate JetStream push subscribing with a delivery subject 
    /// and how the delivery subject can be used as a subject of a core Nats message.
    /// </summary>
    internal static class JetStreamPushSubscribeDeliverSubject
    {
        private const string Usage =
            "Usage: JetStreamPushSubscribeFilterSubject [-url url] [-creds file] [-strm stream] [-sub subject-prefix] [-deliver deliver-prefix]" +
            "\n\nDefault Values:" +
            "\n   [-stream]  ds-stream" +
            "\n   [-subject] ds-subject-" +
            "\n   [-deliver] ds-target-";
        
        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("NATS JetStream Push Subscribe Bind Durable", args, Usage)
                .DefaultStream("fs-stream")
                .DefaultSubject("fs-subject")
                .Build();

            string subjectNoAck = helper.Subject + "noack";
            string subjectAck = helper.Subject + "ack";
            string deliverNoAck = helper.DeliverSubject + "noack";
            string deliverAck = helper.DeliverSubject + "ack";

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Create a JetStreamManagement context.
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                    // Use the utility to create a stream stored in memory.
                    JsUtils.CreateStreamExitWhenExists(jsm, helper.Stream, subjectNoAck, subjectAck);
                    
                    // Create our JetStream context.
                    IJetStream js = c.CreateJetStreamContext();

                    // The server uses the delivery subject as both an inbox for a JetStream subscription
                    // and as a core nats messages subject.
                    // BUT BE CAREFUL. THIS IS STILL A JETSTREAM SUBJECT AND ALL MESSAGES
                    // ADHERE TO THE ACK POLICY

                    // NoAck 1. Set up the noAck consumer / deliver subject configuration
                    ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                        .WithAckPolicy(AckPolicy.None)
                        .WithAckWait(1000)
                        .Build();

                    PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                        .WithDeliverSubject(deliverNoAck)
                        .WithConfiguration(cc)
                        .Build();

                    // NoAck 2. Set up the JetStream and core subscriptions
                    //          Notice the JetStream subscribes to the real subject
                    //          and the core subscribes to the delivery subject
                    //          Order matters, you must do the JetStream subscribe first
                    //          But you also must make sure the core sub is made
                    //          before messages are published
                    IJetStreamPushSyncSubscription jsSub = js.PushSubscribeSync(subjectNoAck, pso);
                    c.Flush(5000);
                    ISyncSubscription coreSub = c.SubscribeSync(deliverNoAck);

                    // NoAck 3. JsUtils.Publish to the real subject
                    JsUtils.Publish(js, subjectNoAck, "A", 1);

                    // NoAck 4. Read the message with the js no ack subscription. No need to ack
                    Msg msg = jsSub.NextMessage(1000);
                    PrintMessage("\nNoAck 4. Read w/JetStream sub", msg);

                    // NoAck 5. Read the message with the core subscription on the
                    //          no ack deliver subject. Since this message is a JetStream
                    //          message we could ack. But we don't have to since the consumer
                    //          was setup as AckPolicy None
                    msg = coreSub.NextMessage(1000);
                    PrintMessage("NoAck 5. Read w/core sub", msg);

                    // NoAck 6. Thread.Sleep longer than the ack wait period to check and make sure the
                    //     message is not replayed
                    Thread.Sleep(1100);
                    
                    try
                    {
                        coreSub.NextMessage(1000);
                        Console.WriteLine("NoAck 6. NOPE! Should not have gotten here");
                    }
                    catch (NATSTimeoutException) // timeout means there are no messages available
                    {
                        Console.WriteLine("NoAck 6. Read w/core sub.\nAck Policy is none so no replay even though message was not Ack'd.\nThere was no message available.");
                    }

                    // Ack 1. Set up the Ack consumer / deliver subject configuration
                    cc = ConsumerConfiguration.Builder()
                            .WithAckPolicy(AckPolicy.Explicit)
                            .WithAckWait(1000)
                            .Build();

                    pso = PushSubscribeOptions.Builder()
                            .WithDeliverSubject(deliverAck)
                            .WithConfiguration(cc)
                            .Build();

                    // Ack 2. Set up the JetStream and core subscriptions
                    jsSub = js.PushSubscribeSync(subjectAck, pso);
                    c.Flush(5000);
                    coreSub = c.SubscribeSync(deliverAck);

                    // Ack 3. JsUtils.Publish to the real subject
                    JsUtils.Publish(js, subjectAck, "B", 1);

                    // Ack 4. Read the message with the js no ack subscription. No need to ack
                    msg = jsSub.NextMessage(1000);
                    PrintMessage("\nAck 4. Read w/JetStream sub", msg);

                    // Ack 5. Read the message with the core subscription on the
                    //        ack deliver subject.
                    //        Even though it is read on a core subscription
                    //        it still is a JetStream message. Don't ack this time
                    msg = coreSub.NextMessage(1000);
                    PrintMessage("Ack 5. Read w/core sub", msg);

                    // Ack 6. Thread.Sleep longer than the ack wait period to check and
                    //        see that the message is re-delivered. Ack this time.
                    Thread.Sleep(1100);
                    msg = coreSub.NextMessage(1000);
                    msg.Ack();
                    PrintMessage("Ack 6. Read w/core sub.\nWasn't Ack'd after step 'Ack 5.' so message was replayed.", msg);

                    // Ack 7. Thread.Sleep longer than the ack wait period. The message
                    //        is not re-delivered this time
                    Thread.Sleep(1100);
                    
                    try
                    {
                        coreSub.NextMessage(1000);
                        Console.WriteLine("Ack 7. NOPE! Should not have gotten here");
                    }
                    catch (NATSTimeoutException) // timeout means there are no messages available
                    {
                        Console.WriteLine("Ack 7. Read w/core sub.\nMessage received by core sub in step 'Ack 6.' was JetStream so it was Ack'd and therefore not replayed.\nThere was no message available.", msg);
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

        private static void PrintMessage(string label, Msg msg) {
            Console.WriteLine(label);
            if (msg == null) {
                Console.WriteLine("  Message: null");
            }
            else {
                Console.WriteLine("  Message: " + msg);
                Console.WriteLine("  JetStream: " + msg.IsJetStream);
                Console.WriteLine("  Meta: " + msg.MetaData);
            }
            Console.WriteLine();
        }
    }
}
