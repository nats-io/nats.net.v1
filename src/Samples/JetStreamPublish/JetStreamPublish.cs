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
    /// This example will demonstrate JetStream publishing.
    /// </summary>
    internal static class JetStreamPublish
    {
        private const string Usage = 
            "Usage: JetStreamPublish [-url url] [-creds file] [-stream stream] " +
            "[-subject subject] [-count count] [-payload payload] [-header key:value]" +
            "\n\nDefault Values:" +
            "\n   [-stream]   example-stream" +
            "\n   [-subject]  example-subject" +
            "\n   [-payload]  Hello" +
            "\n   [-count]    10" +
            "\n\nRun Notes:" +
            "\n   - count < 1 is the same as 1" +
            "\n   - quote multi word payload" +
            "\n   - headers are optional, quote multi word value, no colons ':' in value please!";

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("JetStream Publish", args, Usage)
                .DefaultStream("example-stream")
                .DefaultSubject("example-subject")
                .DefaultPayload("Hello")
                .DefaultCount(10)
                .Build();

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Use the utility to create a stream stored in memory.
                    JsUtils.CreateStreamOrUpdateSubjects(c, helper.Stream, helper.Subject);

                    // create a JetStream context
                    IJetStream js = c.CreateJetStreamContext();

                    int stop = helper.Count < 2 ? 2 : helper.Count + 1;
                    for (int x = 1; x < stop; x++)
                    {
                        // make unique message data if you want more than 1 message
                        byte[] data = helper.Count < 2
                            ? Encoding.UTF8.GetBytes(helper.Payload)
                            : Encoding.UTF8.GetBytes(helper.Payload + "-" + x);

                        // Publish a message and print the results of the publish acknowledgement.
                        Msg msg = new Msg(helper.Subject, null, helper.Header, data);

                        // We'll use the defaults for this simple example, but there are options
                        // to constrain publishing to certain streams, expect sequence numbers and
                        // more. See the JetStreamPublishWithOptionsUseCases example for details.
                        // An exception will be thrown if there is a failure.
                        PublishAck pa = js.Publish(msg);
                        Console.WriteLine("Published message '{0}' on subject '{1}', stream '{2}', seqno '{3}'.",
                            Encoding.UTF8.GetString(data), helper.Subject, pa.Stream, pa.Seq);
                    }
                }
            }
            catch (Exception ex)
            {
                helper.ReportException(ex);
            }
        }
    }
}
