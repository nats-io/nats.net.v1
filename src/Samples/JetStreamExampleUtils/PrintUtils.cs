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
using NATS.Client.JetStream;

namespace NATSExamples
{
    public static class PrintUtils
    {
        public static void PrintStreamInfo(StreamInfo si) {
            PrintObject(si, "StreamConfiguration", "StreamState", "ClusterInfo", "Mirror", "SourceInfos");
        }

        public static void PrintStreamInfoList(IList<StreamInfo> list) {
            bool first = true;
            foreach (StreamInfo si in list)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    Console.WriteLine();
                }
                PrintStreamInfo(si);
            }
        }

        public static void PrintConsumerInfo(ConsumerInfo ci) {
            PrintObject(ci, "!ConsumerInfo", "ConsumerConfiguration", "Delivered", "AckFloor", "ClusterInfo");
        }

        public static void PrintConsumerInfoList(IList<ConsumerInfo> list)
        {
            bool first = true;
            foreach (ConsumerInfo ci in list)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    Console.WriteLine();
                }
                PrintConsumerInfo(ci);
            }
        }

        public static void PrintObject(object o, params string[] subObjectNames) {
            string s = o.ToString();
            foreach (string sub in subObjectNames) {
                bool noIndent = sub.StartsWith("!");
                string sb = noIndent ? sub.Substring(1) : sub;
                string rx1 = ", " + sb;
                string repl1 = (noIndent ? ",\n": ",\n    ") + sb;
                s = s.Replace(rx1, repl1);
            }

            Console.WriteLine(s);
        }
    }
}