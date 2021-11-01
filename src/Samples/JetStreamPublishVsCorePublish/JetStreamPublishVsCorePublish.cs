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
    /// This example will demonstrate the ability to publish to a stream with either
    /// the JetStream.publish(...) or with core Connection.publish(...)
    /// 
    /// The difference lies in the whether it's important to your application to receive
    /// a publish ack and whether or not you want to set publish expectations.
    /// </summary>
    internal static class JetStreamPublishVsCorePublish
    {
        private const string Usage = 
            "Usage: JetStreamPublishVsCorePublish [-url url] [-creds file] [-stream stream] [-subject subject]" +
            "\n\nDefault Values:" +
            "\n   [-stream]  js-or-core-stream" +
            "\n   [-subject] js-or-core-subject";
        
        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("JetStream Publish Vs Core Publish", args, Usage)
                .DefaultStream("js-or-core-stream")
                .DefaultSubject("js-or-core-subject")
                .Build();

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Create a JetStreamManagement context.
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                    
                    // Use the utility to create a stream stored in memory.
                    JsUtils.CreateStreamExitWhenExists(jsm, helper.Stream, helper.Subject);

                    // create a JetStream context
                    IJetStream js = c.CreateJetStreamContext();

                    // Regular Nats publish is straightforward
                    c.Publish(helper.Subject, Encoding.ASCII.GetBytes("regular-message"));

                    // A JetStream publish allows you to set publish options
                    // that a regular publish does not.
                    // A JetStream publish returns an ack of the publish. There
                    // is no ack in a regular message.
                    Msg msg = new Msg(helper.Subject, Encoding.ASCII.GetBytes("js-message"));

                    PublishAck pa = js.Publish(msg);
                    Console.WriteLine(pa);

                    // set up the subscription
                    IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(helper.Subject);
                    c.Flush(500); // flush outgoing communication with/to the server
                    
                    // Both messages appear in the stream as JetStream messages
                    msg = sub.NextMessage(500);
                    msg.Ack();
                    Console.WriteLine("Received Data: '" + Encoding.ASCII.GetString(msg.Data) + "'\n         Meta: " + msg.MetaData);

                    msg = sub.NextMessage(500);
                    msg.Ack();
                    Console.WriteLine("Received Data: '" + Encoding.ASCII.GetString(msg.Data) + "'\n         Meta: " + msg.MetaData);
                    
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