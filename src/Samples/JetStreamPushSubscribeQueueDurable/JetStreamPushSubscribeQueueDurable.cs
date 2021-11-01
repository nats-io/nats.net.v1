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
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    internal static class JetStreamPushSubscribeQueueDurable
    {
        private const string Usage =
            "Usage: JetStreamPushSubscribeQueueDurable [-url url] [-creds file] [-stream stream] " +
            "[-subject subject] [-queue queue] [-durable durable] [-deliver deliverSubject] [-count count] [-subscount numsubs]" +
            "\n\nDefault Values:" +
            "\n   [-stream]    qdur-stream" +
            "\n   [-subject]   qdur-subject" +
            "\n   [-queue]     qdur-queue" +
            "\n   [-durable]   qdur-durable" +
            "\n   [-count]     100" +
            "\n   [-subscount] 5";
        
        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("NATS JetStream Push Subscribe Bind Durable", args, Usage)
                .DefaultStream("qdur-stream")
                .DefaultSubject("qdur-subject")
                .DefaultQueue("qdur-queue")
                .DefaultDurable("qdur-durable")
                .DefaultCount(100)
                .DefaultSubsCount(5)
                .Build();

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Create a JetStreamManagement context.
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                    
                    // Use the utility to create a stream stored in memory.
                    JsUtils.CreateStreamExitWhenExists(jsm, helper.Stream, helper.Subject);

                    // TODO
                    
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
