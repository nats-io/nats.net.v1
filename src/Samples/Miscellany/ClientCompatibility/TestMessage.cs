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
using NATS.Client;

namespace NATSExamples.ClientCompatibility
{
    public class TestMessage
    {
        // info from the message
        public readonly string subject;
        public readonly string replyTo;
        public readonly byte[] payload;
    
        // info from the subject
        public readonly Suite suite;
        public readonly Test? test;
        public readonly string something;
        public readonly Kind? kind;
    
        public TestMessage(Msg m) {
            replyTo = m.Reply;
            subject = m.Subject; // tests.<suite>.<test>.<something>.[command|result]
            payload = m.Data;
    
            String[] split = subject.Split('.');
    
            this.suite = CompatibilityEnums.GetSuite(split[1]);
            if (split.Length > 2) {
                this.test = CompatibilityEnums.GetTest(split[2]);
                this.something = split[3];
                this.kind = CompatibilityEnums.GetKind(split[4]);
            }
            else {
                this.test = null;
                this.something = null;
                this.kind = null;
            }
        }
    
        protected TestMessage(TestMessage tm) {
            this.subject = tm.subject;
            this.replyTo = tm.replyTo;
            this.payload = tm.payload;
            this.suite = tm.suite;
            this.test = tm.test;
            this.something = tm.something;
            this.kind = tm.kind;
        }
    }
}
