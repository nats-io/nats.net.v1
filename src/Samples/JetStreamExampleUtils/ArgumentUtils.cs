﻿// Copyright 2021 The NATS Authors
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

namespace JetStreamExampleUtils
{
    public class ArgumentHelper
    {
        private string _title;
        
        public string Url { get; set; }
        public string Creds { get; set; }
        public string Stream { get; set; }
        public string Subject { get; set; }
        public string Durable { get; set; }
        public string Payload { get; set; }
        public int Count { get; set; }
        public MsgHeader Headers { get; set; }

        public ArgumentHelper(string title)
        {
            _title = title;
            Url = Defaults.Url;
            Count = int.MinValue;
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

                    case "-payload": 
                        Payload = args[i + 1];
                        break;

                    case "-count": 
                        Count = Convert.ToInt32(args[i + 1]);
                        break;

                    case "-header": 
                        string[] split = args[i + 1].Split(':');
                        if (split.Length != 2) { UsageThenExit(usage); }
                        Headers.Add(split[0], split[1]);
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
            Console.WriteLine($"{_title} Example");
            _banner("Url", Url);
            _banner("Stream", Stream);
            _banner("Subject", Subject);
            _banner("Count", Count);
            _banner("Payload", Payload);
            _banner("Headers", Headers?.Count > 0 ? "Yes" : null);
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
            if (value > int.MinValue)
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

        public ArgumentHelperBuilder Url(string s)
        {
            argumentHelper.Url = s;
            return this;
        }

        public ArgumentHelperBuilder Creds(string s)
        {
            argumentHelper.Creds = s;
            return this;
        }

        public ArgumentHelperBuilder Stream(string s)
        {
            argumentHelper.Stream = s;
            return this;
        }

        public ArgumentHelperBuilder Subject(string s)
        {
            argumentHelper.Subject = s;
            return this;
        }

        public ArgumentHelperBuilder Durable(string s)
        {
            argumentHelper.Durable = s;
            return this;
        }

        public ArgumentHelperBuilder Payload(string s)
        {
            argumentHelper.Payload = s;
            return this;
        }

        public ArgumentHelperBuilder Header(MsgHeader h)
        {
            argumentHelper.Headers = h;
            return this;
        }

        public ArgumentHelperBuilder Count(int i)
        {
            argumentHelper.Count = i;
            return this;
        }
    }
}