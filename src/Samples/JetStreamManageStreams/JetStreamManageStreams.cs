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
using System.Collections.Generic;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    /// <summary>
    /// This example will demonstrate JetStream management (admin) api stream management.
    /// </summary>
    internal static class JetStreamManageStreams
    {
        private const string Usage = 
            "Usage: JetStreamManageStreams [-url url] [-creds file] [-stream stream-prefix] [-subject subject-prefix]" +
            "\n\nDefault Values:" +
            "\n   [-stream]   manage-stream-" +
            "\n   [-subject]  manage-subject-";

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("JetStream Manage Streams", args, Usage)
                .DefaultStream("manage-stream-")
                .DefaultSubject("manage-subject-")
                .Build();
            
            string stream1 = helper.Stream + "1";
            string stream2 = helper.Stream + "2";
            string subject1 = helper.Subject + "1";
            string subject2 = helper.Subject + "2";
            string subject3 = helper.Subject + "3";
            string subject4 = helper.Subject + "4";

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // Create a JetStreamManagement context.
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                    // we want to be able to completely create and delete the streams
                    // so don't want to work with existing streams
                    JsUtils.ExitIfStreamExists(jsm, stream1);
                    JsUtils.ExitIfStreamExists(jsm, stream2);

                    // 1. Create (add) a stream with a subject
                    Console.WriteLine("\n----------\n1. Configure And Add Stream 1");
                    StreamConfiguration streamConfig = StreamConfiguration.Builder()
                            .WithName(stream1)
                            .WithSubjects(subject1)
                            // .WithRetentionPolicy(...)
                            // .WithMaxConsumers(...)
                            // .WithMaxBytes(...)
                            // .WithMaxAge(...)
                            // .WithMaxMsgSize(...)
                            .WithStorageType(StorageType.Memory)
                            // .WithReplicas(...)
                            // .WithNoAck(...)
                            // .WithTemplateOwner(...)
                            // .WithDiscardPolicy(...)
                            .Build();

                    StreamInfo streamInfo = jsm.AddStream(streamConfig);
                    PrintUtils.PrintStreamInfo(streamInfo);

                    // 2. Update stream, in this case add a subject
                    //    Thre are very few properties that can actually
                    // -  StreamConfiguration is immutable once created
                    // -  but the builder can help with that.
                    Console.WriteLine("----------\n2. Update Stream 1");
                    streamConfig = StreamConfiguration.Builder(streamInfo.Config)
                            .AddSubjects(subject2).Build();
                    streamInfo = jsm.UpdateStream(streamConfig);
                    PrintUtils.PrintStreamInfo(streamInfo);

                    // 3. Create (add) another stream with 2 subjects
                    Console.WriteLine("----------\n3. Configure And Add Stream 2");
                    streamConfig = StreamConfiguration.Builder()
                            .WithName(stream2)
                            .WithSubjects(subject3, subject4)
                            .WithStorageType(StorageType.Memory)
                            .Build();
                    streamInfo = jsm.AddStream(streamConfig);
                    PrintUtils.PrintStreamInfo(streamInfo);

                    // 4. Get information on streams
                    // 4.0 publish some message for more interesting stream state information
                    // -   SUBJECT1 is associated with STREAM1
                    // 4.1 getStreamInfo on a specific stream
                    // 4.2 get a list of all streams
                    // 4.3 get a list of StreamInfo's for all streams
                    Console.WriteLine("----------\n4.1 getStreamInfo");
                    JsUtils.Publish(c, subject1, 5);
                    streamInfo = jsm.GetStreamInfo(stream1);
                    PrintUtils.PrintStreamInfo(streamInfo);

                    Console.WriteLine("----------\n4.2 getStreamNames");
                    IList<String> streamNames = jsm.GetStreamNames();
                    PrintUtils.PrintObject(streamNames);

                    Console.WriteLine("----------\n4.3 getStreams");
                    IList<StreamInfo> streamInfos = jsm.GetStreams();
                    PrintUtils.PrintStreamInfoList(streamInfos);

                    // 5. Purge a stream of it's messages
                    Console.WriteLine("----------\n5. Purge stream");
                    PurgeResponse purgeResponse = jsm.PurgeStream(stream1);
                    PrintUtils.PrintObject(purgeResponse);

                    // 6. Delete the streams
                    // Subsequent calls to getStreamInfo, deleteStream or purgeStream
                    // will throw a JetStreamApiException "stream not found [10059]"
                    Console.WriteLine("----------\n6. Delete streams");
                    jsm.DeleteStream(stream1);
                    jsm.DeleteStream(stream2);

                    // 7. Try to delete the consumer again and get the exception
                    Console.WriteLine("----------\n7. Delete stream again");
                    try
                    {
                        jsm.DeleteStream(stream1);
                    }
                    catch (NATSJetStreamException e)
                    {
                        Console.WriteLine($"Exception was: '{e.ErrorDescription}'");
                    }

                    Console.WriteLine("----------\n");
                }
            }
            catch (Exception ex)
            {
                helper.ReportException(ex);
            }
        }
    }
}