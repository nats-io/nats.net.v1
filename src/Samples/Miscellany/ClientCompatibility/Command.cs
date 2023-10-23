// Copyright 2023 The NATS Authors
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
using System.Text;
using NATS.Client;
using NATS.Client.Internals.SimpleJSON;

namespace NATSExamples.ClientCompatibility
{
    public abstract class Command : TestMessage
    {
        public readonly IConnection c;
    
        // info from the message data
        public readonly JSONNode full;
        public readonly JSONNode config;
        
        protected Command(IConnection c, TestMessage tm) : base(tm) {
            this.c = c;
            JSONNode tempDataValue = null;
            JSONNode tempConfig = null;
            if (payload != null && payload.Length > 0) {
                tempDataValue = JSON.Parse(Encoding.UTF8.GetString(payload));
                Log.info("CMD", subject, tempDataValue.ToString());
                tempConfig = tempDataValue["config"];
            }
            full = tempDataValue;
            config = tempConfig;
        }

        protected void respond()
        {
            Log.info($"RESPOND {subject}");
            c.Publish(replyTo, null);
        }

        protected void respond(string payload)
        {
            Log.info($"RESPOND {subject} with {payload}");
            c.Publish(replyTo, Encoding.UTF8.GetBytes(payload));
        }

        public override string ToString()
        {
            return full.ToString();
        }

        protected void handleException(Exception e) {
            Log.error(subject, e);
        }
    }
}
