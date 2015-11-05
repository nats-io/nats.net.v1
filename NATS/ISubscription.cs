// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NATS.Client
{
    /// <summary>
    /// Represents interest in a NATS topic.
    /// </summary>
    public interface ISubscription
    {
        /// <summary>
        /// Gets the subject of interest.
        /// </summary>
        string Subject { get; }

        /// <summary>
        /// Gets the name of the queue groups this subscriber belongs to.
        /// </summary>
        /// <remarks>
        /// Optional queue group name. If present, all subscriptions with the
        /// same name will form a distributed queue, and each message will
        /// only be processed by one member of the group.
        /// </remarks>
        string Queue { get; }


        /// <summary>
        /// Gets the Connection this subscriber was created on.
        /// </summary>
        Connection Connection { get; }

        /// <summary>
        /// True if the subscription is active, false otherwise.
        /// </summary>
        bool IsValid { get; }


        /// <summary>
        /// Removes interest in the given subject.
        /// </summary>
        void Unsubscribe();

        /// <summary>
        /// AutoUnsubscribe will issue an automatic Unsubscribe that is
        /// processed by the server when max messages have been received.
        /// This can be useful when sending a request to an unknown number
        /// of subscribers. Request() uses this functionality.
        /// </summary>
        /// <param name="max">Number of messages to receive before unsubscribing.</param>
        void AutoUnsubscribe(int max);

        /// <summary>
        /// Gets the number of messages received, but not processed,
        /// this subscriber.
        /// </summary>
        int QueuedMessageCount { get; }
    }
}
