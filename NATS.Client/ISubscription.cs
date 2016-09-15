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

        /// <summary>
        /// Sets the message limit and bytes limit of a subscriber.
        /// </summary>
        /// <param name="messageLimit">Maximum number of pending messages to allow.</param>
        /// <param name="bytesLimit">Maximum number of pending bytes to allow.</param> 
        void SetPendingLimits(long messageLimit, long bytesLimit);

        /// <summary>
        /// Gets or sets the maximum number of pending bytes to allow.
        /// </summary>
        long PendingByteLimit { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of pending messages to allow.
        /// </summary>
        long PendingMessageLimit { get; set; }

        /// <summary>
        /// Gets pending messages and bytes.
        /// </summary>
        /// <param name="pendingBytes">Current number of pending bytes.</param>
        /// <param name="pendingMessages">Current number of pending messages.</param>
        void GetPending(out long pendingBytes, out long pendingMessages);

        /// <summary>
        /// Gets the current number of pending bytes.
        /// </summary>
        long PendingBytes { get; }

        /// <summary>
        /// Gets the current number of pending messages.
        /// </summary>
        long PendingMessages { get; }

        /// <summary>
        /// Gets the maximum pending messages and pending bytes recorded.
        /// </summary>
        /// <param name="maxPendingBytes">Maximum number of pending bytes recorded.</param>
        /// <param name="maxPendingMessages">Maximum number of pending messages recorded.</param>
        void GetMaxPending(out long maxPendingBytes, out long maxPendingMessages);

        /// <summary>
        /// Gets the maximum number of pending bytes recorded.
        /// </summary>
        long MaxPendingBytes { get; }

        /// <summary>
        /// Gets the maximum number of pending messages recorded.
        /// </summary>
        long MaxPendingMessages { get; }

        /// <summary>
        /// Clears the maximum pending statistics.
        /// </summary>
        void ClearMaxPending();

        /// <summary>
        /// Gets the number of messages delivered to a subscriber.
        /// </summary>
        long Delivered { get; }

        /// <summary>
        /// Gets the number of message dropped by violations of pending limits.
        /// </summary>
        /// <remarks>
        /// If the server declares the connection a slow consumer, this number may 
        /// not be valid.
        /// </remarks>
        long Dropped { get; }
    }
}
