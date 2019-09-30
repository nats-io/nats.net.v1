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

using System.Threading.Tasks;

namespace NATS.Client
{
    /// <summary>
    /// Represents interest in a NATS topic.
    /// </summary>
    /// <remarks>
    /// <para>Subscriptions represent interest in a topic on a NATS Server or cluster of
    /// NATS Servers. Subscriptions can be exact or include wildcards. A subscriber can
    /// process a NATS message synchronously (<see cref="ISyncSubscription"/>) or asynchronously
    /// (<see cref="IAsyncSubscription"/>).</para>
    /// </remarks>
    /// <seealso cref="ISyncSubscription"/>
    /// <seealso cref="IAsyncSubscription"/>
    public interface ISubscription
    {
        /// <summary>
        /// Gets the subject for this subscription.
        /// </summary>
        /// <remarks><para>Subject names, including reply subject (INBOX) names, are case-sensitive
        /// and must be non-empty alphanumeric strings with no embedded whitespace, and optionally
        /// token-delimited using the dot character (<c>.</c>), e.g.: <c>FOO</c>, <c>BAR</c>,
        /// <c>foo.BAR</c>, <c>FOO.BAR</c>, and <c>FOO.BAR.BAZ</c> are all valid subject names, while:
        /// <c>FOO. BAR</c>, <c>foo. .bar</c> and <c>foo..bar</c> are <em>not</em> valid subject names.</para>
        /// <para>NATS supports the use of wildcards in subject subscriptions.</para>
        /// <list>
        /// <item>The asterisk character (<c>*</c>) matches any token at any level of the subject.</item>
        /// <item>The greater than symbol (<c>&gt;</c>), also known as the <em>full wildcard</em>, matches
        /// one or more tokens at the tail of a subject, and must be the last token. The wildcard subject
        /// <c>foo.&gt;</c> will match <c>foo.bar</c> or <c>foo.bar.baz.1</c>, but not <c>foo</c>.</item>
        /// <item>Wildcards must be separate tokens (<c>foo.*.bar</c> or <c>foo.&gt;</c> are syntactically
        /// valid; <c>foo*.bar</c>, <c>f*o.b*r</c> and <c>foo&gt;</c> are not).</item>
        /// </list>
        /// <para>For example, the wildcard subscrpitions <c>foo.*.quux</c> and <c>foo.&gt;</c> both match
        /// <c>foo.bar.quux</c>, but only the latter matches <c>foo.bar.baz</c>. With the full wildcard,
        /// it is also possible to express interest in every subject that may exist in NATS (<c>&gt;</c>).</para>
        /// </remarks>
        string Subject { get; }

        /// <summary>
        /// Gets the optional queue group name.
        /// </summary>
        /// <remarks>
        /// <para>If present, all subscriptions with the same name will form a distributed queue, and each message will only
        /// be processed by one member of the group. Although queue groups have multiple subscribers,
        /// each message is consumed by only one.</para>
        /// </remarks>
        string Queue { get; }

        /// <summary>
        /// Gets the <see cref="NATS.Client.Connection"/> associated with this instance.
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

        /// <summary>
        /// Drains a subscription for gracefully unsubscribing.
        /// </summary>
        /// <remarks>
        /// This method unsubscribes the subscriber and drains all
        /// remaining messages.
        /// </remarks>
        /// <seealso cref="Unsubscribe()"/>
        void Drain();

        /// <summary>
        /// Drains a subscription for gracefully unsubscribing.
        /// </summary>
        /// <param name="timeout">The duration in milliseconds to wait while draining.</param>    
        /// /// <seealso cref="Unsubscribe()"/>
        void Drain(int timeout);

        /// <summary>
        /// Drains a subscription for gracefully unsubscribing.
        /// </summary>
        /// <remarks>
        /// This method unsubscribes the subscriber and drains all
        /// remaining messages.
        /// </remarks>
        /// <seealso cref="Unsubscribe()"/>
        /// <returns>A task that represents the asynchronous drain operation.</returns>
        Task DrainAsync();

        /// <summary>
        /// Drains a subscription for gracefully unsubscribing.
        /// </summary>
        /// <param name="timeout">The duration in milliseconds to wait while draining.</param>    
        /// /// <seealso cref="Unsubscribe()"/>
        /// <returns>A task that represents the asynchronous drain operation.</returns>
        Task DrainAsync(int timeout);
    }

}
