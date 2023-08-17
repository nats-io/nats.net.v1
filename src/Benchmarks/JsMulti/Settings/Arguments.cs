// Copyright 2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
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
using System.IO;
using System.Linq;
using JsMulti.Settings;
using NATS.Client.JetStream;

namespace JsMulti
{
    public class Arguments
    {
        public const string Individual = "individual";
        public const string Shared = "shared";

        private IList<string> args = new List<string>();

        public static Arguments Instance() { return new Arguments(); }
        public static Arguments Instance(string subject) { return Instance().Subject(subject); }
        public static Arguments PubSync(string subject) { return Instance().Action(JsmAction.PubSync).Subject(subject); }
        public static Arguments PubAsync(string subject) { return Instance().Action(JsmAction.PubAsync).Subject(subject); }
        public static Arguments PubCore(string subject) { return Instance().Action(JsmAction.PubCore).Subject(subject); }
        public static Arguments SubPush(string subject) { return Instance().Action(JsmAction.SubPush).Subject(subject); }
        public static Arguments SubQueue(string subject) { return Instance().Action(JsmAction.SubQueue).Subject(subject); }
        public static Arguments SubPull(string subject) { return Instance().Action(JsmAction.SubPull).Subject(subject); }
        public static Arguments SubPullQueue(string subject) { return Instance().Action(JsmAction.SubPullQueue).Subject(subject); }

        private Arguments Add(string option) {
            args.Add("-" + option);
            return this;
        }

        private Arguments Add(string option, Object value) {
            args.Add("-" + option);
            args.Add(value.ToString());
            return this;
        }

        public Arguments Action(JsmAction action) {
            return Add("a", action);
        }

        public Arguments Server(string server) {
            return Add("s", server);
        }

        public Arguments LatencyFlag() {
            return Add("lf");
        }

        public Arguments LatencyFlag(bool lf) {
            return lf ? Add("lf") : this;
        }

        // todo
        // public ArgumentBuilder OptionsFactory(string optionsFactoryClassName) {
            // return Add("of", optionsFactoryClassName);
        // }

        public Arguments ReportFrequency(int reportFrequency) {
            return Add("rf", reportFrequency);
        }

        public Arguments NoReporting() {
            return Add("rf", -1);
        }

        public Arguments StorageType(StorageType storageType) {
            return Add("o", storageType);
        }

        public Arguments Memory() {
            return StorageType(NATS.Client.JetStream.StorageType.Memory);
        }

        public Arguments File() {
            return StorageType(NATS.Client.JetStream.StorageType.File);
        }

        public Arguments Replicas(int replicas) {
            return Add("c", replicas);
        }

        public Arguments Subject(string subject) {
            if (subject == null) {
                return this;
            }
            return Add("u", subject);
        }

        public Arguments MessageCount(int messageCount) {
            return Add("m", messageCount);
        }

        public Arguments Threads(int threads) {
            return Add("d", threads);
        }

        public Arguments IndividualConnection() {
            return Add("n", Individual);
        }

        public Arguments SharedConnection() {
            return Add("n", Shared);
        }

        public Arguments SharedConnection(bool shared) {
            return Add("n", shared ? Shared : Individual);
        }

        public Arguments Jitter(long jitter) {
            return Add("j", jitter);
        }

        public Arguments PayloadSize(int payloadSize) {
            return Add("ps", payloadSize);
        }

        public Arguments RoundSize(int roundSize) {
            return Add("rs", roundSize);
        }

        public Arguments AckPolicy(AckPolicy ackPolicy) {
            return Add("kp", ackPolicy);
        }

        public Arguments AckExplicit() {
            return AckPolicy(NATS.Client.JetStream.AckPolicy.Explicit);
        }

        public Arguments AckNone() {
            return AckPolicy(NATS.Client.JetStream.AckPolicy.None);
        }

        public Arguments AckAll() {
            return AckPolicy(NATS.Client.JetStream.AckPolicy.All);
        }

        public Arguments AckAllFrequency(int ackAllFrequency) {
            return Add("kf", ackAllFrequency);
        }

        public Arguments BatchSize(int batchSize) {
            return Add("bs", batchSize);
        }
 
        public string[] ToArray()
        {
            return args.ToArray();
        }

        public void PrintCommandLine(TextWriter ps) {
            foreach (string a in args) {
                ps.Write(a + " ");
            }
            ps.WriteLine("");
        }

        public void PrintCommandLine() {
            PrintCommandLine(Console.Out);
        }
   }
}