﻿// Copyright 2024 The NATS Authors
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
using NATS.Client;

namespace NATSExamples
{
    public class ChaosCommandLine
    {
        public readonly string Servers;
        public readonly string Stream;
        public readonly string Subject;
        public readonly int Runtime;
        public readonly int PubJitter;
        public readonly bool create;
        public readonly bool R3;
        public readonly bool Publish;
        public readonly bool Debug;
        public readonly bool Work;
        public readonly IList<ChaosCommandLineConsumer> CommandLineConsumers;

        private void Usage()
        {
        }

        private String AsString(String val)
        {
            return val.Trim();
        }

        private int AsNumber(String name, String val, long upper)
        {
            int v;
            if (!int.TryParse(val, out v))
            {
                throw new ArgumentException("Invalid number for " + name + ": " + val);
            }

            if (upper == -2 && v < 1)
            {
                return int.MaxValue;
            }

            if (upper > 0)
            {
                if (v > upper)
                {
                    throw new ArgumentException("Value for " + name + " cannot exceed " + upper);
                }
            }

            return v;
        }

        private long AsNumber(String name, String val, int lower, int upper)
        {
            int v;
            if (!int.TryParse(val, out v))
            {
                throw new ArgumentException("Invalid number for " + name + ": " + val);
            }

            if (v < lower)
            {
                throw new ArgumentException("Value for " + name + " cannot be less than " + lower);
            }

            if (v > upper)
            {
                throw new ArgumentException("Value for " + name + " cannot exceed " + upper);
            }

            return v;
        }

        public ChaosCommandLine(string[] args)
        {
            string _servers = "";
            string _stream = "app-stream";
            string _subject = "app-subject";
            int _runtime = -1;
            int _pubJitter = 50;
            bool _create = false;
            bool _r3 = false;
            bool _publish = false;
            bool _debug = false;
            bool _work = false;
            IList<ChaosCommandLineConsumer> _commandLineConsumers = new List<ChaosCommandLineConsumer>();

            if (args != null && args.Length > 0)
            {
                try
                {
                    for (int x = 0; x < args.Length; x++)
                    {
                        String arg = args[x].Trim();
                        if (arg.Length == 0)
                        {
                            continue;
                        }

                        switch (arg)
                        {
                            case "--servers":
                                _servers = AsString(args[++x]);
                                break;
                            case "--stream":
                                _stream = AsString(args[++x]);
                                break;
                            case "--subject":
                                _subject = AsString(args[++x]);
                                break;
                            case "--runtime":
                                _runtime = AsNumber("runtime", args[++x], -1) * 1000;
                                break;
                            case "--pubjitter":
                                _pubJitter = AsNumber("pubjitter", args[++x], -1);
                                break;
                            case "--create":
                                _create = true;
                                break;
                            case "--r3":
                                _r3 = true;
                                break;
                            case "--publish":
                                _publish = true;
                                break;
                            case "--debug":
                                _debug = true;
                                break;
                            case "--work":
                                _work = true;
                                break;
                            case "--simple":
                            case "--iterate":
                            case "--fetch":
                                String temp = args[++x];
                                if (temp.Contains(","))
                                {
                                    String[] split = temp.Split(',');
                                    _commandLineConsumers.Add(new ChaosCommandLineConsumer(
                                        arg.Substring(2),
                                        split[0],
                                        AsNumber("batchSize", split[1], -1),
                                        AsNumber("expiresInMs", split[2], -1)
                                    ));
                                }
                                else
                                {
                                    _commandLineConsumers.Add(new ChaosCommandLineConsumer(
                                        arg.Substring(2),
                                        temp,
                                        AsNumber("batchSize", args[++x], -1),
                                        AsNumber("expiresInMs", args[++x], -1)
                                    ));
                                }
                                break;
                            case "--push":
                                _commandLineConsumers.Add(new ChaosCommandLineConsumer(args[++x]));
                                break;
                            default:
                                throw new ArgumentException("Unknown argument: " + arg);
                        }
                    }
                }
                catch (Exception e)
                {
                    Usage();
                    throw e;
                }
                
                if (!_create && !_publish && _commandLineConsumers.Count == 0) {
                    throw new ArgumentException("Consumer commands are required if not creating or publishing");
                }

                Servers = _servers;
                Stream = _stream;
                Subject = _subject;
                Runtime = _runtime;
                create = _create;
                R3 =_r3;
                Publish = _publish;
                Debug = _debug;
                Work = _work;
                PubJitter = _pubJitter;
                CommandLineConsumers = _commandLineConsumers;

            }
        }
        
        private void append(StringBuilder sb, String label, Object value, bool test) {
            if (test) {
                sb.Append("--").Append(label).Append(" ").Append(value).Append(" ");
            }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("Chaos Test App Config ");
            append(sb, "servers", string.Join(",", Servers), true);
            append(sb, "stream", Stream, true);
            append(sb, "subject", Subject, true);
            append(sb, "runtime", Runtime, true);
            append(sb, "create", create, create);
            append(sb, "R3", R3, R3);
            append(sb, "publish", Publish, Publish);
            append(sb, "pubjitter", PubJitter, Publish);
            foreach (ChaosCommandLineConsumer cc in CommandLineConsumers) {
                append(sb, "consumer", cc, true);
            }
            return sb.ToString().Trim();
        }

        public Options MakeOptions(Func<string> labelFunc, Action connectChangeEventAction = null)
        {
            Action ccea = connectChangeEventAction == null ? () => {} : connectChangeEventAction;
            
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = Servers;
            opts.MaxReconnect = -1;
            opts.AsyncErrorEventHandler = (obj, a) =>
            {
                ChaosOutput.ControlMessage(labelFunc.Invoke(), $"Error: {a.Error}");
            };
            opts.ReconnectedEventHandler = (obj, a) =>
            {
                ccea.Invoke();
                ChaosOutput.ControlMessage(labelFunc.Invoke(), $"Reconnected to {a.Conn.ConnectedUrl}");
            };
            opts.DisconnectedEventHandler = (obj, a) =>
            {
                ccea.Invoke();
                ChaosOutput.ControlMessage(labelFunc.Invoke(), "Disconnected.");
            };
            opts.ClosedEventHandler = (obj, a) =>
            {
                ccea.Invoke();
                ChaosOutput.ControlMessage(labelFunc.Invoke(), "Connection closed.");
            };
            opts.ServerDiscoveredEventHandler = (obj, a) =>
            {
                ChaosOutput.ControlMessage(labelFunc.Invoke(), "Server Discovered");
            };
            opts.LameDuckModeEventHandler = (obj, a) =>
            {
                ChaosOutput.ControlMessage(labelFunc.Invoke(), "Lame Duck Mode");
            };
            opts.HeartbeatAlarmEventHandler = (obj, a) =>
            {
                ChaosOutput.ControlMessage(labelFunc.Invoke(), "Heartbeat Alarm");
            };
            opts.UnhandledStatusEventHandler = (obj, a) =>
            {
                ChaosOutput.ControlMessage(labelFunc.Invoke(), "Unhandled Status");
            };
            opts.PullStatusErrorEventHandler = (obj, a) =>
            {
                ChaosOutput.ControlMessage(labelFunc.Invoke(), "Pull Status Error");
            };
            opts.FlowControlProcessedEventHandler = (obj, a) =>
            {
                ChaosOutput.ControlMessage(labelFunc.Invoke(), "Flow Control Processed");
            };
            opts.PullStatusWarningEventHandler = (obj, a) => { };

            return opts;
        }
    }
}
