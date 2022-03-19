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
using System.Collections.Concurrent;
using System.Text;
using JsMulti.Shared;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using static JsMulti.Shared.Utils;

namespace JsMulti.Settings
{
    public class Context
    {
        public JsmAction Action { get; }
        public bool LatencyFlag { get; }
        public string Server { get; }
        public int ReportFrequency { get; }
        public string Subject { get; }
        public int MessageCount { get; }
        public int Threads { get; }
        public bool ConnShared { get; }
        public int Jitter { get; }
        public int PayloadSize { get; }
        public int RoundSize { get; }
        public AckPolicy AckPolicy { get; }
        public int AckAllFrequency { get; }
        public int BatchSize { get; }
        
        // once per context for now
        public readonly string QueueName = "qn" + UniqueEnough();

        // constant now but might change in the future
        public readonly int MaxPubRetries = 10;
        public readonly int AckWaitSeconds = 120;

        // ----------------------------------------------------------------------------------------------------
        // macros / state / vars to access through methods instead of direct
        // ----------------------------------------------------------------------------------------------------
        private IOptionsFactory _optionsFactory;
        private readonly int[] _perThread;
        private readonly byte[] _payload;
        private readonly string _subDurableWhenQueue= "qd" + UniqueEnough();
        private readonly ConcurrentDictionary<string, InterlockedLong> _subscribeCounters = new ConcurrentDictionary<string, InterlockedLong>();

        public void SetOptionsFactory(IOptionsFactory optionsFactory) {
            _optionsFactory = optionsFactory;
        }

        public Options GetOptions() {
            return _optionsFactory.GetOptions(this);
        }

        public JetStreamOptions GetJetStreamOptions() {
            return _optionsFactory.GetJetStreamOptions(this);
        }

        public byte[] GetPayload() {
            return _payload;
        }
        
        public string GetSubDurable(int durableId) {
            // is a queue, use the same durable
            // not a queue, each durable is unique
            return Action.IsQueue ? _subDurableWhenQueue : "dur" + UniqueEnough() + durableId;
        }
        
        public int GetPubCount(int id) {
            return _perThread[id-1]; // ids start at 1
        }

        public InterlockedLong GetSubscribeCounter(string key) {
            return _subscribeCounters.GetOrAdd(key, k => new InterlockedLong());
        }

        public string GetLabel(int id) {
            return Action + (ConnShared ? "Shared" : "Individual") + id;
        }

        // ----------------------------------------------------------------------------------------------------
        // ToString
        // ----------------------------------------------------------------------------------------------------
        public void Print() {
            Console.WriteLine(this + "\n");
        }
        
        private const string TsTmpl = "\n  {0,-26}{1}";
        private void Append(StringBuilder sb, string label, string arg, Object value, bool test) {
            if (test) {
                sb.Append(string.Format(TsTmpl, label + " (-" + arg + "):", value));
            }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("JetStream Multi-Tool Run Config:");
            Append(sb, "action", "a", Action, true);
            Append(sb, "action", "lf", "Yes", LatencyFlag);
            Append(sb, "options factory", "of", _optionsFactory.GetType().FullName, true);
            Append(sb, "report frequency", "rf", ReportFrequency == Int32.MaxValue ? "no reporting" : "" + ReportFrequency, true);
            Append(sb, "subject", "u", Subject, true);
            Append(sb, "message count", "m", MessageCount, true);
            Append(sb, "threads", "d", Threads, true);
            Append(sb, "connection strategy", "n", ConnShared ? Arguments.Shared : Arguments.Individual, Threads > 1);

            Append(sb, "payload size", "p", PayloadSize + " bytes", Action.IsPubAction);
            Append(sb, "jitter", "j", Jitter, Action.IsPubAction);

            Append(sb, "round size", "r", RoundSize, Action.IsPubAction && Action.IsPubSync);

            Append(sb, "ack policy", "kp", AckPolicy, Action.IsSubAction);
            Append(sb, "ack all frequency", "kf", AckAllFrequency, Action.IsSubAction);
            Append(sb, "ack all frequency", "kf", AckAllFrequency, Action.IsPush && AckPolicy == AckPolicy.All);

            Append(sb, "batch size", "b", BatchSize, Action.IsPull);

            return sb.ToString();
        }

        // ----------------------------------------------------------------------------------------------------
        // Construction
        // ----------------------------------------------------------------------------------------------------
        public Context(Arguments args) : this(args.ToArray()) {}
        
        public Context(string[] args)
        {
            if (args == null || args.Length == 0) {
                Exit();
            }

            JsmAction _action = null;
            bool _latencyFlag = false;
            string _server = Defaults.Url;
            string _optionsFactoryTypeName = null;
            int _reportFrequency = 10000;
            string _subject = "sub" + UniqueEnough();
            int _messageCount = 100_000;
            int _threads = 1;
            bool _connShared = true;
            int _jitter = 0;
            int _payloadSize = 128;
            int _roundSize = 100;
            AckPolicy _ackPolicy = AckPolicy.Explicit;
            int _ackAllFrequency = 1;
            int _batchSize = 10;

            if (args != null && args.Length > 0) {
                try
                {
                    for (int x = 0; x < args.Length; x++)
                    {
                        string arg = args[x].Trim();
                        switch (arg)
                        {
                            case "-s":
                                _server = AsString(args[++x]);
                                break;
                            case "-of":
                                _optionsFactoryTypeName = AsString(args[++x]);
                                break;
                            case "-a":
                                _action = JsmAction.GetInstance(AsString(args[++x]));
                                if (_action == null)
                                {
                                    Error("Valid action required!");
                                }

                                break;
                            case "-lf":
                                _latencyFlag = true;
                                break;
                            case "-u":
                                _subject = AsString(args[++x]);
                                break;
                            case "-m":
                                _messageCount = AsNumber("total messages", args[++x], -1);
                                break;
                            case "-ps":
                                _payloadSize = AsNumber("payload size", args[++x], 1048576);
                                break;
                            case "-bs":
                                _batchSize = AsNumber("batch size", args[++x], 200);
                                break;
                            case "-rs":
                                _roundSize = AsNumber("round size", args[++x], 1000);
                                break;
                            case "-d":
                                _threads = AsNumber("number of threads", args[++x], 10);
                                break;
                            case "-j":
                                _jitter = AsNumber("jitter", args[++x], 10_000);
                                break;
                            case "-n":
                                _connShared = TrueFalse("connection strategy", args[++x], "Shared", "Individual");
                                break;
                            case "-kp":
                                AckPolicy? ap = ApiEnums.GetAckPolicy(AsString(args[++x]).ToLower());
                                if (ap == null)
                                {
                                    Error("Invalid Ack Policy, must be one of [explicit, none, all]");
                                }
                                _ackPolicy = ap.Value;
                                break;
                            case "-kf":
                                _ackAllFrequency = AsNumber("ack frequency", args[++x], 100);
                                break;
                            case "-rf":
                                _reportFrequency = AsNumber("report frequency", args[++x], -2);
                                break;
                            case "":
                                break;
                            default:
                                Error("Unknown argument: " + arg);
                                break;
                        }
                    }
                }
                catch (Exception)
                {
                    Error("Exception while parsing, most likely missing an argument value.");
                }
            }

            if (_messageCount < 1) {
                Error("Message count required!");
            }

            if (_threads == 1 && _action.IsQueue) {
                Error("Queue subscribing requires multiple threads!");
            }

            if (_action.IsPull && _ackPolicy != AckPolicy.Explicit) 
            {
                Error("Pull subscribing requires AckPolicy.Explicit!");
            }

            Action = _action;
            LatencyFlag = _latencyFlag;
            Server = _server;
            ReportFrequency = _reportFrequency;
            Subject = _subject;
            MessageCount = _messageCount;
            Threads = _threads;
            ConnShared = _connShared;
            Jitter = _jitter;
            PayloadSize = _payloadSize;
            RoundSize = _roundSize;
            AckPolicy = _ackPolicy;
            AckAllFrequency = _ackAllFrequency;
            BatchSize = _batchSize;
            
            if (_optionsFactoryTypeName == null) {
                _optionsFactory = new OptionsFactory();
            }
            else {
                try {
                    Type t = Type.GetType(_optionsFactoryTypeName);
                    _optionsFactory = Activator.CreateInstance(t) as IOptionsFactory;
                }
                catch (Exception e) {
                    Error("Error creating OptionsFactory: " + e);
                }
            }

            _payload = new byte[PayloadSize];

            int total = 0;
            _perThread = new int[Threads];
            for (int x = 0; x < Threads; x++) {
                _perThread[x] = MessageCount / Threads;
                total += _perThread[x];
            }

            int ix = 0;
            while (total < MessageCount) {
                _perThread[ix++]++;
                total++;
            }
        }

        private static void Error(string errMsg) {
            Console.Error.WriteLine("\nERROR: " + errMsg);
            Exit();
        }

        private static void Exit() {
            Console.Error.WriteLine(Usage.USAGE);
            Environment.Exit(-1);
        }

        private static string Normalize(string s) {
            return s.Replace("_", "").Replace(",", "").Replace("\\.", "");
        }

        private static int AsNumber(string name, string val, int upper) {
            int v = Int32.Parse(Normalize(AsString(val)));
            if (upper == -2 && v < 1) {
                return Int32.MaxValue;
            }
            if (upper > 0) {
                if (v > upper) {
                    Error("value for " + name + " cannot exceed " + upper);
                }
            }
            return v;
        }

        private static string AsString(string val) {
            return val.Trim();
        }

        private static bool TrueFalse(string name, string val, string trueChoice, string falseChoice) {
            return Choice(name, val, trueChoice, falseChoice) == 0;
        }

        private static int Choice(string name, string val, params string[] choices) {
            string lower = AsString(val.ToLower());
            for (int x = 0; x < choices.Length; x++) {
                if (lower.Equals(choices[x].ToLower())) {
                    return x;
                }
            }

            Error("Invalid choice for " + name + " [" + lower + "]. Must be one of [" + String.Join(", ", choices) + "]");
            return -1;
        }
    }
}