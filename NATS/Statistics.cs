// Copyright 2015 Apcera Inc. All rights reserved.

using System;

// disable XML comment warnings
#pragma warning disable 1591

namespace NATS.Client
{

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