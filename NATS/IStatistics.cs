// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NATS.Client
{
    /// <summary>
    /// Tracks various statistics received and sent on this connection.
    /// </summary>
    public interface IStatistics
    {
        /// <summary>
        /// Gets the number of inbound messages received.
        /// </summary>
        long InMsgs { get; }

        /// <summary>
        /// Gets the number of messages sent.
        /// </summary>
        long OutMsgs { get; }

        /// <summary>
        /// Gets the number of incoming bytes.
        /// </summary>
        long InBytes { get; }

        /// <summary>
        /// Gets the outgoing number of bytes.
        /// </summary>
        long OutBytes { get; }

        /// <summary>
        /// Gets the number of reconnections.
        /// </summary>
        long Reconnects { get; }
    }
}
