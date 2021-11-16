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
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    /// <summary>
    /// This example will demonstrate JetStream basic asynchronous push subscribing with a handler.
    /// Run NatsJsPub first to setup message data.
    /// </summary>
    internal static class JetStreamPushSubscribeBasicAsync
    {
        private const string Usage =
            "Usage: JetStreamPushSubscribeBasicAsync [-url url] [-creds file] [-stream stream] " +
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
            ArgumentHelper helper = new ArgumentHelperBuilder("Push Subscribe Basic Async", args, Usage)
                .DefaultStream("example-stream")
                .DefaultSubject("example-subject")
                .Build();

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    JsUtils.CreateStreamWhenDoesNotExist(c, helper.Stream, helper.Subject);

                    IJetStream js = c.CreateJetStreamContext();

                    new Thread(() =>
                    {
                        js.PushSubscribeAsync(helper.Subject, (sender, a) =>
                        {
                            a.Message.Ack();
                            Console.WriteLine(Encoding.UTF8.GetString(a.Message.Data));
                        }, false);
                    }).Start();
                    
                    Thread.Sleep(3000);
                }
            }
            catch (Exception ex)
            {
                helper.ReportException(ex);
            }
        }
    }
}
