// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NATS.Client
{
    /// <summary>
    /// An object of this class is an asynchronous subscription representing interest
    /// in a subject.   The subject can have wildcards (partial:*, full:>).
    /// Messages will be delivered to the associated MsgHandler event delegates.
    /// While nothing prevents event handlers from being added or 
    /// removed while processing messages, no messages will be received until
    /// Start() has been called.  This allows all event handlers to be added
    /// before message processing begins.
    /// </summary>
    /// <remarks><see cref="MsgHandler">See MsgHandler</see>.</remarks>
    public interface IAsyncSubscription : ISubscription, IDisposable
    {
        /// <summary>
        /// Adds or removes a message handlers for this subscriber.
        /// </summary>
        /// <remarks><see cref="MsgHandler">See MsgHandler</see></remarks>
        event MsgHandler MessageHandler;

        /// <summary>
        /// This completes the subsciption process notifying the server this subscriber
        /// has interest.
        /// </summary>
        void Start();
    }
}
