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

namespace NATSExamples
{
    public class ArgumentHelper
    {
        private string _title;
        internal bool _countUnlimitedFlag;
        
        public string Url { get; set; } = Defaults.Url;
        public string Creds { get; set; }
        public string Stream { get; set; }
        public string Subject { get; set; }
        public string Queue { get; set; }
        public string Payload { get; set; }
        public string Consumer { get; set; }
        public string Durable { get; set; }
        public string DeliverSubject { get; set; }
        public int Count { get; set; } = int.MinValue;
        public int SubsCount { get; set; } = int.MinValue;
        public int PullSize { get; set; } = int.MinValue;
        public MsgHeader Header { get; set; }
        public string Bucket { get; set; }
        public string Description { get; set; }

        public ArgumentHelper(string title)
        {
            _title = title;
        }
  
        public void Parse(string[] args, string usage)
        {
            if (args == null) { return; } // will just use defaults
            if (args.Length % 2 == 1) { UsageThenExit(usage); } // odd number of arguments
            
            for (int i = 0; i < args.Length; i += 2)
            {
                switch (args[i])
                {
                    case "-url":
                        Url = args[i + 1];
                        break;

                    case "-creds": 
                        Creds = args[i + 1];
                        break;

                    case "-stream": 
                        Subject = args[i + 1];
                        break;

                    case "-subject": 
                        Subject = args[i + 1];
                        break;

                    case "-count": 
                        Count = Convert.ToInt32(args[i + 1]);
                        break;

                    case "-subscount": 
                        SubsCount = Convert.ToInt32(args[i + 1]);
                        break;

                    case "-pull": 
                        PullSize = Convert.ToInt32(args[i + 1]);
                        break;

                    case "-queue": 
                        Queue = args[i + 1];
                        break;

                    case "-payload": 
                        Payload = args[i + 1];
                        break;

                    case "-con": 
                        Consumer = args[i + 1];
                        break;

                    case "-dur": 
                        Durable = args[i + 1];
                        break;

                    case "-deliver": 
                        DeliverSubject = args[i + 1];
                        break;

                    case "-header": 
                        string[] split = args[i + 1].Split(':');
                        if (split.Length != 2) { UsageThenExit(usage); }
                        Header.Add(split[0], split[1]);
                        break;

                    case "-buk": 
                        Consumer = args[i + 1];
                        break;

                    case "-desc": 
                        Consumer = args[i + 1];
                        break;
                }
            }
        }

        public Options MakeOptions()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = Url;
            if (Creds != null)
            {
                opts.SetUserCredentials(Creds);
            }

            return opts;
        }
 
        public void ReportException(Exception ex)
        {
            Console.Error.WriteLine($"Exception running {_title} Example");
            Console.Error.WriteLine($"    {ex.Message}");
            Console.Error.WriteLine($"    {ex}");
        }
        
        public void DisplayBanner()
        {
            Console.WriteLine($"\n{_title} Example");
            _banner("Url", Url);
            _banner("Stream", Stream);
            _banner("Subject", Subject);
            _banner("bucket", Bucket);
            _banner("description", Description);
            _banner("Queue", Queue);
            _banner("Payload", Payload);
            _banner("Consumer", Consumer);
            _banner("Durable", Durable);
            _banner("Deliver", DeliverSubject);
            _banner("Creds", Creds == null ? null : "**********");
            _banner("PullSize", PullSize);
            _banner("Count", Count, _countUnlimitedFlag);
            _banner("SubsCount", SubsCount);
            _banner("Headers", Header?.Count > 0 ? Header.Count : int.MinValue);

            Console.WriteLine();
        }

        private void _banner(string label, string value)
        {
            if (value != null)
            {
                Console.WriteLine($"  {label}: {value}");
            }
        }

        private void _banner(string label, int value)
        {
            _banner(label, value, false);
        }

        private void _banner(string label, int value, bool unlimited)
        {
            if (unlimited && (value == int.MaxValue || value < 1)) {
                Console.WriteLine($"  {label}: Unlimited");
            }
            else if (value > int.MinValue)
            {
                Console.WriteLine($"  {label}: {value}");
            }
        }

        public void UsageThenExit(string usage)
        {
            Console.Error.WriteLine(usage);
            Environment.Exit(-1);
        }
    }

    public class ArgumentHelperBuilder
    {
        private ArgumentHelper argumentHelper;
        private string[] args;
        private string usage;
        
        public ArgumentHelperBuilder(string title, string[] args, string usage)
        {
            argumentHelper = new ArgumentHelper(title);
            this.args = args;
            this.usage = usage;
        }

        public ArgumentHelper Build()
        {
            argumentHelper.Parse(args, usage);
            argumentHelper.DisplayBanner();
            return argumentHelper;
        }

        public ArgumentHelperBuilder DefaultStream(string s)
        {
            argumentHelper.Stream = s;
            return this;
        }

        public ArgumentHelperBuilder DefaultSubject(string s)
        {
            argumentHelper.Subject = s;
            return this;
        }

        public ArgumentHelperBuilder DefaultDurable(string s)
        {
            argumentHelper.Durable = s;
            return this;
        }

        public ArgumentHelperBuilder DefaultQueue(string s)
        {
            argumentHelper.Queue = s;
            return this;
        }

        public ArgumentHelperBuilder DefaultDeliverSubject(string s)
        {
            argumentHelper.DeliverSubject = s;
            return this;
        }

        public ArgumentHelperBuilder DefaultPayload(string s)
        {
            argumentHelper.Payload = s;
            return this;
        }
        
        public ArgumentHelperBuilder DefaultCount(int i)
        {
            argumentHelper.Count = i;
            return this;
        }
        
        public ArgumentHelperBuilder DefaultSubsCount(int i)
        {
            argumentHelper.SubsCount = i;
            return this;
        }
        
        public ArgumentHelperBuilder DefaultCount(int i, bool unlimitedFlag)
        {
            argumentHelper.Count = i;
            argumentHelper._countUnlimitedFlag = unlimitedFlag;
            return this;
        }

        public ArgumentHelperBuilder DefaultBucket(string s)
        {
            argumentHelper.Bucket = s;
            return this;
        }

        public ArgumentHelperBuilder DefaultDescription(string s)
        {
            argumentHelper.Description = s;
            return this;
        }
        
    }
}