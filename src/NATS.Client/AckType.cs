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

using System.Text;

namespace NATS.Client
{
    public sealed class AckType
    {
        public static AckType AckAck = new AckType("+ACK", true);
        public static AckType AckNak = new AckType("-NAK", true);
        public static AckType AckProgress = new AckType("+WPI", false);
        public static AckType AckTerm = new AckType("+TERM", true);

        public string Text { get; }
        public byte[] Bytes { get; }
        public bool IsTerminal { get; }

        public AckType(string text, bool terminal)
        {
            Text = text;
            Bytes = Encoding.ASCII.GetBytes(text);
            IsTerminal = terminal;
        }
 
        public byte[] BodyBytes(long delayNanoseconds) {
            return delayNanoseconds < 1 ? Bytes : Encoding.ASCII.GetBytes($"{Text} {{\"delay\": {delayNanoseconds}}}");
        }
    }
}
