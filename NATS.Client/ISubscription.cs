// Copyright 2015 Apcera Inc. All rights reserved.

namespace NATS.Client
{
    /// <summary>
    /// Represents interest in a NATS topic.
    /// </summary>
    public interface ISubscription
    {
        /// <summary>
        /// Gets the subject for this subscription.
        /// </summary>
        string Subject { get; }

        /// <summary>
        /// Gets the optional queue group name.
        /// </summary>
        /// <remarks>
        /// If present, all subscriptions with the same name will form a distributed queue, and each message will only
        /// be processed by one member of the group.
        /// </remarks>
        string Queue { get; }

        /// <summary>
        /// Gets the <see cref="Connection"/> associated with this instance.
        /// </summary>
        Connection Connection { get; }

        /// <summary>
        /// Gets a value indicating whether or not the <see cref="ISubscription"/> is still valid.
        /// </summary>
        bool IsValid { get; }

        /// <summary>
        /// Removes interest in the <see cref="Subject"/>.
        /// </summary>
        void Unsubscribe();

        /// <summary>
        /// Issues an automatic call to <see cref="Unsubscribe"/> when <paramref name="max"/> messages have been
        /// received.
        /// </summary>
        /// <remarks>This can be useful when sending a request to an unknown number of subscribers.
        /// <see cref="Connection"/>'s Request methods use this functionality.</remarks>
        /// <param name="max">The maximum number of messages to receive on the subscription before calling
        /// <see cref="Unsubscribe"/>. Values less than or equal to zero (<c>0</c>) unsubscribe immediately.</param>
        void AutoUnsubscribe(int max);

        /// <summary>
        /// Gets the number of messages remaining in the delivery queue.
        /// </summary>
        int QueuedMessageCount { get; }

        /// <summary>
        /// Sets the limits for pending messages and bytes for this instance.
        /// </summary>
        /// <remarks>Zero (<c>0</c>) is not allowed. Negative values indicate that the
        /// given metric is not limited.</remarks>
        /// <param name="messageLimit">The maximum number of pending messages.</param>
        /// <param name="bytesLimit">The maximum number of pending bytes of payload.</param>
        void SetPendingLimits(long messageLimit, long bytesLimit);

        /// <summary>
        /// Gets or sets the maximum allowed count of pending bytes.
        /// </summary>
        /// <value>The limit must not be zero (<c>0</c>). Negative values indicate there is no
        /// limit on the number of pending bytes.</value>
        long PendingByteLimit { get; set; }

        /// <summary>
        /// Gets or sets the maximum allowed count of pending messages.
        /// </summary>
        /// <value>The limit must not be zero (<c>0</c>). Negative values indicate there is no
        /// limit on the number of pending messages.</value>
        long PendingMessageLimit { get; set; }

        /// <summary>
        /// Returns the pending byte and message counts.
        /// </summary>
        /// <param name="pendingBytes">When this method returns, <paramref name="pendingBytes"/> will
        /// contain the count of bytes not yet processed on the <see cref="ISubscription"/>.</param>
        /// <param name="pendingMessages">When this method returns, <paramref name="pendingMessages"/> will
        /// contain the count of messages not yet processed on the <see cref="ISubscription"/>.</param>
        void GetPending(out long pendingBytes, out long pendingMessages);

        /// <summary>
        /// Gets the number of bytes not yet processed on this instance.
        /// </summary>
        long PendingBytes { get; }

        /// <summary>
        /// Gets the number of messages not yet processed on this instance.
        /// </summary>
        long PendingMessages { get; }

        /// <summary>
        /// Returns the maximum number of pending bytes and messages during the life of the <see cref="Subscription"/>.
        /// </summary>
        /// <param name="maxPendingBytes">When this method returns, <paramref name="maxPendingBytes"/>
        /// will contain the current maximum pending bytes.</param>
        /// <param name="maxPendingMessages">When this method returns, <paramref name="maxPendingBytes"/>
        /// will contain the current maximum pending messages.</param>
        void GetMaxPending(out long maxPendingBytes, out long maxPendingMessages);

        /// <summary>
        /// Gets the maximum number of pending bytes seen so far by this instance.
        /// </summary>
        long MaxPendingBytes { get; }

        /// <summary>
        /// Gets the maximum number of messages seen so far by this instance.
        /// </summary>
        long MaxPendingMessages { get; }

        /// <summary>
        /// Clears the maximum pending bytes and messages statistics.
        /// </summary>
        void ClearMaxPending();

        /// <summary>
        /// Gets the number of delivered messages for this instance.
        /// </summary>
        long Delivered { get; }

        /// <summary>
        /// Gets the number of known dropped messages for this instance.
        /// </summary>
        /// <remarks>
        /// This will correspond to the messages dropped by violations of
        /// <see cref="PendingByteLimit"/> and/or <see cref="PendingMessageLimit"/>.
        /// If the NATS server declares the connection a slow consumer, the count
        /// may not be accurate.
        /// </remarks>
        long Dropped { get; }
    }
}
