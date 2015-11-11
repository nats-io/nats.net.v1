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
    /// Messages will be delivered to the associated MessageHandler event delegates.
    /// While nothing prevents event handlers from being added or 
    /// removed while processing messages.  If the subscriber was created without
    /// providing a handler, no messages will be received until
    /// Start() has been called.  This allows event handlers to be added
    /// before message processing begins.
    /// </summary>
    public interface IAsyncSubscription : ISubscription, IDisposable
    {
        /// <summary>
        /// Adds or removes a message handler for this subscriber.
        /// </summary>
        event EventHandler<MsgHandlerEventArgs> MessageHandler;

        /// <summary>
        /// Starts receiving messages.
        /// </summary>
        /// <remarks>If a message handler was not passed when
        /// creating the subscriber, the subscriber must assign
        /// delegates then complete the subscription process
        /// with this method.
        /// </remarks>
        void Start();
    }
}
