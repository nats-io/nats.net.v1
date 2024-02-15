using System;
using System.Collections.Generic;
using System.Text;
using NATS.Client;

namespace NATSExamples
{
    public class CommandLine
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
        public readonly IList<CommandLineConsumer> CommandLineConsumers;

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

        public CommandLine(string[] args)
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
            IList<CommandLineConsumer> _commandLineConsumers = new List<CommandLineConsumer>();

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
                            case "--fetch":
                                String temp = args[++x];
                                if (temp.Contains(","))
                                {
                                    String[] split = temp.Split(',');
                                    _commandLineConsumers.Add(new CommandLineConsumer(
                                        arg.Substring(2),
                                        split[0],
                                        AsNumber("batchSize", split[1], -1),
                                        AsNumber("expiresInMs", split[2], -1)
                                    ));
                                }
                                else
                                {
                                    _commandLineConsumers.Add(new CommandLineConsumer(
                                        arg.Substring(2),
                                        temp,
                                        AsNumber("batchSize", args[++x], -1),
                                        AsNumber("expiresInMs", args[++x], -1)
                                    ));
                                }

                                break;
                            case "--push":
                                _commandLineConsumers.Add(new CommandLineConsumer(args[++x]));
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
            foreach (CommandLineConsumer cc in CommandLineConsumers) {
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
                Output.ControlMessage(labelFunc.Invoke(), $"Error: {a.Error}");
            };
            opts.ReconnectedEventHandler = (obj, a) =>
            {
                ccea.Invoke();
                Output.ControlMessage(labelFunc.Invoke(), $"Reconnected to {a.Conn.ConnectedUrl}");
            };
            opts.DisconnectedEventHandler = (obj, a) =>
            {
                ccea.Invoke();
                Output.ControlMessage(labelFunc.Invoke(), "Disconnected.");
            };
            opts.ClosedEventHandler = (obj, a) =>
            {
                ccea.Invoke();
                Output.ControlMessage(labelFunc.Invoke(), "Connection closed.");
            };
            opts.ServerDiscoveredEventHandler = (obj, a) =>
            {
                Output.ControlMessage(labelFunc.Invoke(), "Server Discovered");
            };
            opts.LameDuckModeEventHandler = (obj, a) =>
            {
                Output.ControlMessage(labelFunc.Invoke(), "Lame Duck Mode");
            };
            opts.HeartbeatAlarmEventHandler = (obj, a) =>
            {
                Output.ControlMessage(labelFunc.Invoke(), "Heartbeat Alarm");
            };
            opts.UnhandledStatusEventHandler = (obj, a) =>
            {
                Output.ControlMessage(labelFunc.Invoke(), "Unhandled Status");
            };
            opts.PullStatusErrorEventHandler = (obj, a) =>
            {
                Output.ControlMessage(labelFunc.Invoke(), "Pull Status Error");
            };
            opts.FlowControlProcessedEventHandler = (obj, a) =>
            {
                Output.ControlMessage(labelFunc.Invoke(), "Flow Control Processed");
            };
            opts.PullStatusWarningEventHandler = (obj, a) => { };

            return opts;
        }
    }
}