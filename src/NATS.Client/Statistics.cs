// Copyright 2015-2018 The NATS Authors
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

namespace NATS.Client
{
    /// <summary>
    /// Tracks various statistics received and sent on an <see cref="IConnection"/>.
    /// </summary>
    public class Statistics : IStatistics
    {
        internal Statistics() { }

        internal long inMsgs = 0;

        /// <summary>
        /// Gets the number of inbound messages received.
        /// </summary>
        public long InMsgs
        {
            get { return inMsgs; }
        }

        internal long outMsgs = 0;

        /// <summary>
        /// Gets the number of messages sent.
        /// </summary>
        public long OutMsgs
        {
            get { return outMsgs; }
        }

        internal long inBytes = 0;

        /// <summary>
        /// Gets the number of incoming bytes.
        /// </summary>
        public long InBytes
        {
            get { return inBytes; }
        }

        internal long outBytes = 0;

        /// <summary>
        /// Gets the outgoing number of bytes.
        /// </summary>
        public long OutBytes
        {
            get { return outBytes; }
        }

        internal long reconnects = 0;

        /// <summary>
        /// Gets the number of reconnections.
        /// </summary>
        public long Reconnects
        {
            get {  return reconnects; }
        }

        // deep copy contructor
        internal Statistics(Statistics obj)
        {
            this.inMsgs = obj.inMsgs;
            this.inBytes = obj.inBytes;
            this.outBytes = obj.outBytes;
            this.outMsgs = obj.outMsgs;
            this.reconnects = obj.reconnects;
        }

        internal void clear()
        {
            this.inBytes  = 0;
            this.inMsgs   = 0;
            this.outBytes = 0;
            this.outMsgs  = 0;
        }
    }


}