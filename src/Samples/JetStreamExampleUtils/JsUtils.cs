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
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    public static class JsUtils
    {
        // ----------------------------------------------------------------------------------------------------
        // STREAM INFO / CREATE / UPDATE 
        // ----------------------------------------------------------------------------------------------------
        public static StreamInfo GetStreamInfoOrNullWhenNotExist(IJetStreamManagement jsm, string streamName) {
            try {
                return jsm.GetStreamInfo(streamName);
            }
            catch (NATSJetStreamException e) {
                if (e.ErrorCode == 404) {
                    return null;
                }
                throw;
            }
        }

        public static bool StreamExists(IConnection c, string streamName) {
            return GetStreamInfoOrNullWhenNotExist(c.CreateJetStreamManagementContext(), streamName) != null;
        }

        public static bool StreamExists(IJetStreamManagement jsm, string streamName) {
            return GetStreamInfoOrNullWhenNotExist(jsm, streamName) != null;
        }

        public static void ExitIfStreamExists(IJetStreamManagement jsm, string streamName)
        {
            if (StreamExists(jsm, streamName))
            {
                Console.WriteLine($"\nThe example cannot run since the stream '{streamName}' already exists.\n" +
                                  "It depends on the stream being in a new state. You can either:\n" +
                                  "  1) Change the stream name in the example.\n  2) Delete the stream.\n  3) Restart the server if the stream is a memory stream.");
                Environment.Exit(-1);
            }
        }

        public static void ExitIfStreamNotExists(IConnection c, string streamName) {
            if (!StreamExists(c, streamName)) {
                Console.WriteLine("\nThe example cannot run since the stream '" + streamName + "' does not exist.\n" +
                                   "It depends on the stream existing and having data.");
                Environment.Exit(-1);
            }
        }

        public static StreamInfo CreateStream(IJetStreamManagement jsm, string streamName, StorageType storageType, params string[] subjects) {
            // Create a stream, here will use a file storage type, and one subject,
            // the passed subject.
            StreamConfiguration sc = StreamConfiguration.Builder()
                .WithName(streamName)
                .WithStorageType(storageType)
                .WithSubjects(subjects)
                .Build();

            // Add or use an existing stream.
            StreamInfo si = jsm.AddStream(sc);
            Console.WriteLine("Created stream '{0}' with subject(s) [{1}]\n", streamName, string.Join(",", si.Config.Subjects));
            return si;
        }

        public static StreamInfo CreateStream(IJetStreamManagement jsm, string stream, params string[] subjects) {
            return CreateStream(jsm, stream, StorageType.Memory, subjects);
        }

        public static StreamInfo CreateStream(IConnection c, string stream, params string[] subjects) {
            return CreateStream(c.CreateJetStreamManagementContext(), stream, StorageType.Memory, subjects);
        }

        public static StreamInfo CreateStreamExitWhenExists(IConnection c, string streamName, params string[] subjects) {
            return CreateStreamExitWhenExists(c.CreateJetStreamManagementContext(), streamName, subjects);
        }

        public static StreamInfo CreateStreamExitWhenExists(IJetStreamManagement jsm, string streamName, params string[] subjects)
        {
            ExitIfStreamExists(jsm, streamName);
            return CreateStream(jsm, streamName, StorageType.Memory, subjects);
        }

        public static void CreateStreamWhenDoesNotExist(IJetStreamManagement jsm, string stream, params string[] subjects)
        {
            try
            {
                jsm.GetStreamInfo(stream); // this throws if the stream does not exist
                return;
            }
            catch (NATSJetStreamException)
            {
                /* stream does not exist */
            }

            StreamConfiguration sc = StreamConfiguration.Builder()
                .WithName(stream)
                .WithStorageType(StorageType.Memory)
                .WithSubjects(subjects)
                .Build();
            jsm.AddStream(sc);
        }

        public static void CreateStreamWhenDoesNotExist(IConnection c, string stream, params string[] subjects)
        {
            CreateStreamWhenDoesNotExist(c.CreateJetStreamManagementContext(), stream, subjects);
        }

        public static StreamInfo CreateStreamOrUpdateSubjects(IJetStreamManagement jsm, string streamName, StorageType storageType, params string[] subjects) {

            StreamInfo si = GetStreamInfoOrNullWhenNotExist(jsm, streamName);
            if (si == null) {
                return CreateStream(jsm, streamName, storageType, subjects);
            }

            // check to see if the configuration has all the subject we want
            StreamConfiguration sc = si.Config;
            bool needToUpdate = false;
            foreach (string sub in subjects) {
                if (!sc.Subjects.Contains(sub)) {
                    needToUpdate = true;
                    sc.Subjects.Add(sub);
                }
            }

            if (needToUpdate) {
                si = jsm.UpdateStream(sc);
                Console.WriteLine("Existing stream '{0}' was updated, has subject(s) [{1}]\n",
                    streamName, string.Join(",", si.Config.Subjects));
                // Existing stream 'scratch'  [sub1, sub2]
            }
            else
            {
                Console.WriteLine("Existing stream '{0}' already contained subject(s) [{1}]\n",
                    streamName, string.Join(",", si.Config.Subjects));
            }

            return si;
        }

        public static StreamInfo CreateStreamOrUpdateSubjects(IJetStreamManagement jsm, string streamName, params string[] subjects) {
            return CreateStreamOrUpdateSubjects(jsm, streamName, StorageType.Memory, subjects);
        }

        public static StreamInfo CreateStreamOrUpdateSubjects(IConnection c, string stream, params string[] subjects) {
            return CreateStreamOrUpdateSubjects(c.CreateJetStreamManagementContext(), stream, StorageType.Memory, subjects);
        }

        // ----------------------------------------------------------------------------------------------------
        // PUBLISH
        // ----------------------------------------------------------------------------------------------------
        public static void Publish(IConnection c, string subject, int count) {
            Publish(c.CreateJetStreamContext(), subject, "data", count, false);
        }

        public static void Publish(IJetStream js, String subject, int count) {
            Publish(js, subject, "data", count, false);
        }

        public static void Publish(IJetStream js, String subject, String prefix, int count, bool verbose = true) {
            if (verbose) {
                Console.Write("Publish ->");
            }
            for (int x = 1; x <= count; x++) {
                String data = prefix + x;
                if (verbose) {
                    Console.Write(" " + data);
                }
                js.Publish(subject, Encoding.UTF8.GetBytes(data));
            }
            if (verbose) {
                Console.WriteLine(" <-");
            }
        }

        public static void PublishInBackground(IJetStream js, String subject, String prefix, int count) {
            new Thread(() => {
                try {
                    for (int x = 1; x <= count; x++) {
                        js.Publish(subject, Encoding.ASCII.GetBytes(prefix + "-" + x));
                    }
                } catch (Exception e) {
                    Console.WriteLine(e);
                    Environment.Exit(-1);
                }
            }).Start();
            Thread.Sleep(100); // give the publish thread a little time to get going
        }

        // ----------------------------------------------------------------------------------------------------
        // READ MESSAGES
        // ----------------------------------------------------------------------------------------------------
        public static IList<Msg> ReadMessagesAck(ISyncSubscription sub, bool verbose = true, int timeout = 1000) {
            if (verbose)
            {
                Console.Write("Read/Ack ->");
            }
            IList<Msg> messages = new List<Msg>();
            bool keepGoing = true;
            while (keepGoing)
            {
                try
                {
                    Msg msg = sub.NextMessage(timeout);
                    messages.Add(msg);
                    msg.Ack();
                    if (verbose)
                    {
                        Console.Write(" " + msg.Subject + " / " + Encoding.UTF8.GetString(msg.Data));
                    }
                }
                catch (NATSTimeoutException) // timeout means there are no messages available
                {
                    keepGoing = false;
                }
            }

            if (verbose)
            {
                Console.Write(messages.Count == 0 ? " No messages available <-\n" : " <-\n");
            }

            return messages;
        }
 
        // ----------------------------------------------------------------------------------------------------
        // REPORT
        // ----------------------------------------------------------------------------------------------------
        public static void Report(IList<Msg> list) {
            Console.Write("Fetch ->");
            foreach (Msg m in list) {
                Console.Write(" " + Encoding.UTF8.GetString(m.Data));
            }
            Console.Write(" <- \n");
        }
    }
}