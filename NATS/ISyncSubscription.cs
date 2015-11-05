// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NATS.Client
{
    /// <summary>
    /// A Syncronous Subscripion will express interest in a given subject.
    /// The subject can have wildcards (partial:*, full:>).
    /// Messages arriving are retrieved via NextMsg()
    /// </summary>
    public interface ISyncSubscription : ISubscription, IDisposable
    {
        /// <summary>
        /// This method will return the next message available to a synchronous subscriber
        /// or block until one is available.
        /// </summary>
        /// <returns>a NATS message</returns>
        Msg NextMessage();

        /// <summary>
        /// This method will return the next message available to a synchronous subscriber
        /// or block until one is available. A timeout can be used to return when no
        /// message has been delivered.
        /// </summary>
        /// <remarks>
        /// A timeout of 0 will return null immediately if there are no messages.
        /// </remarks>
        /// <param name="timeout">Timeout value</param>
        /// <returns>a NATS message</returns>
        Msg NextMessage(int timeout);
    }
}
