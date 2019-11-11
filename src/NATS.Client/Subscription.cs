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

using System;
using System.Text;
using System.Threading.Tasks;

// disable XML comment warnings
#pragma warning disable 1591

namespace NATS.Client
{
    /// <summary>
    /// Represents interest in a NATS topic. This class should
    /// not be used directly.
    /// </summary>
    public class Subscription : ISubscription, IDisposable
    {
        readonly  internal  object mu = new object(); // lock

        internal  long           sid = 0; // subscriber ID.
        private   long           msgs;
        internal  long           delivered;
        private   long           bytes;
        internal  long           max = -1;

        // slow consumer
        internal bool       sc   = false;

        internal Connection conn = null;
        internal bool closed = false;
        internal bool connClosed = false;

        internal Channel<Msg> mch = null;
        internal bool ownsChannel = true;

        // Pending stats, async subscriptions, high-speed etc.
        internal long pMsgs = 0;
	    internal long pBytes = 0;
	    internal long pMsgsMax = 0;
	    internal long pBytesMax = 0;
        internal long pMsgsLimit = Defaults.SubPendingMsgsLimit;
	    internal long pBytesLimit = Defaults.SubPendingBytesLimit;
        internal long dropped = 0;

        // Subject that represents this subscription. This can be different
        // than the received subject inside a Msg if this is a wildcard.
        private string      subject = null;

        internal Subscription(Connection conn, string subject, string queue)
        {
            this.conn = conn;
            this.subject = subject;
            this.queue = queue;
        }

        internal virtual void close()
        {
            close(true);
        }

        internal void close(bool closeChannel)
        {
            lock (mu)
            {
                if (closeChannel && mch != null)
                {
                    mch.close();
                    mch = null;
                }
                closed = true;
                connClosed = true;
            }
        }

        /// <summary>
        /// Gets the subject for this subscription.
        /// </summary>
        public string Subject
        {
            get { return subject; }
        }

        // Optional queue group name. If present, all subscriptions with the
        // same name will form a distributed queue, and each message will
        // only be processed by one member of the group.
        string queue;

        /// <summary>
        /// Gets the optional queue group name.
        /// </summary>
        /// <remarks>
        /// If present, all subscriptions with the same name will form a distributed queue, and each message will only
        /// be processed by one member of the group.
        /// </remarks>
        public string Queue
        {
            get { return queue; }
        }

        /// <summary>
        /// Gets the <see cref="Connection"/> associated with this instance.
        /// </summary>
        public Connection Connection
        {
            get
            {
                return conn;
            }
        }
 
        //caller must lock
        internal bool tallyMessage(long bytes)
        {
            if (max > 0 && msgs > max)
                return true;

            this.msgs++;
            this.bytes += bytes;

            return false;
        }

        /// <summary>
        /// Called by <see cref="NATS.Client.Connection"/> when a <see cref="Msg"/> is received, returning
        /// a value indicating if the <see cref="NATS.Client.Connection"/> should keep the subscription
        /// after processing.
        /// </summary>
        /// <param name="msg">A <see cref="Msg"/> received by the <see cref="Subscription"/>.</param>
        /// <returns><c>true</c> if-and-only-if the <see cref="Subscription"/> should remain active;
        /// otherwise <c>false</c> if the <see cref="NATS.Client.Connection"/> should remove this
        /// instance.</returns>
        internal virtual bool processMsg(Msg msg)
        {
            return true;
        }

        private void handleSlowConsumer(Msg msg)
        {
            dropped++;
            conn.processSlowConsumer(this);
            pMsgs--;
            pBytes -= msg.Data.Length;
        }

        /// <summary>
        /// Implementors should call this method when <paramref name="msg"/> has been
        /// delivered to an <see cref="ISubscription"/>.
        /// </summary>
        /// <remarks>Caller must lock on <see cref="mu"/>.</remarks>
        /// <param name="msg">The <see cref="Msg"/> object delivered to a
        /// <see cref="ISubscription"/>.</param>
        /// <returns>The total number of delivered messages.</returns>
        protected long tallyDeliveredMessage(Msg msg)
        {
            delivered++;
            pBytes -= msg.Data.Length;
            pMsgs--;

            return delivered;
        }

        // returns false if the message could not be added because
        // the channel is full, true if the message was added
        // to the channel.
        internal bool addMessage(Msg msg, int maxCount)
        {
            // Subscription internal stats
	        pMsgs++;
	        if (pMsgs > pMsgsMax)
            {
		        pMsgsMax = pMsgs;
            }
	
	        pBytes += msg.Data.Length;
	        if (pBytes > pBytesMax)
            {
		        pBytesMax = pBytes;
            }
	
            // Check for a Slow Consumer
	        if ((pMsgsLimit > 0 && pMsgs > pMsgsLimit)
                || (pBytesLimit > 0 && pBytes > pBytesLimit))
            {
                // slow consumer
                handleSlowConsumer(msg);
                return false;
            }

            if (mch != null)
            {
                if (mch.Count >= maxCount)
                {
                    handleSlowConsumer(msg);
                    return false;
                }
                else
                {
                    sc = false;
                    mch.add(msg);
                }
            }
            return true;
        }

        /// <summary>
        /// Gets a value indicating whether or not the <see cref="Subscription"/> is still valid.
        /// </summary>
        public bool IsValid
        {
            get
            {
                lock (mu)
                {
                    return (conn != null) && !closed;
                }
            }
        }

        internal void unsubscribe(bool throwEx)
        {
            Connection c;
            bool isClosed;
            lock (mu)
            {
                c = this.conn;
                isClosed = this.closed;
            }

            if (c == null)
            {
                if (throwEx)
                    throw new NATSBadSubscriptionException();

                return;
            }

            if (c.IsClosed())
            {
                if (throwEx)
                    throw new NATSConnectionClosedException();
            }

            if (isClosed)
            {
                if (throwEx)
                    throw new NATSBadSubscriptionException();
            }

            if (c.IsDraining())
            {
                if (throwEx)
                    throw new NATSConnectionDrainingException();

                return;
            }

            c.unsubscribe(this, 0, false, 0);
        }

        /// <summary>
        /// Removes interest in the <see cref="Subject"/>.
        /// </summary>
        /// <exception cref="NATSBadSubscriptionException">There is no longer an associated <see cref="Connection"/></exception>
        /// <exception cref="NATSConnectionDrainingException">The <see cref="Connection"/> is draining.
        /// for this <see cref="ISubscription"/>.</exception>
        public virtual void Unsubscribe()
        {
            unsubscribe(true);
        }

        /// <summary>
        /// Issues an automatic call to <see cref="Unsubscribe"/> when <paramref name="max"/> messages have been
        /// received.
        /// </summary>
        /// <remarks>This can be useful when sending a request to an unknown number of subscribers.
        /// <see cref="Connection"/>'s Request methods use this functionality.</remarks>
        /// <param name="max">The maximum number of messages to receive on the subscription before calling
        /// <see cref="Unsubscribe"/>. Values less than or equal to zero (<c>0</c>) unsubscribe immediately.</param>
        /// <exception cref="NATSBadSubscriptionException">There is no longer an associated <see cref="Connection"/>
        /// for this <see cref="ISubscription"/>.</exception>
        public virtual void AutoUnsubscribe(int max)
        {
            Connection c = null;

            lock (mu)
            {
                if (conn == null)
                    throw new NATSBadSubscriptionException();

                if (conn.IsClosed())
                    throw new NATSConnectionClosedException();

                if (closed)
                    throw new NATSBadSubscriptionException();

                c = conn;
            }

            c.unsubscribe(this, max, false, 0);
        }

        /// <summary>
        /// Gets the number of messages remaining in the delivery queue.
        /// </summary>
        /// <exception cref="NATSBadSubscriptionException">There is no longer an associated <see cref="Connection"/>
        /// for this <see cref="ISubscription"/>.</exception>
        public int QueuedMessageCount
        {
            get
            {
                lock (mu)
                {
                    if (conn == null || closed)
                        throw new NATSBadSubscriptionException();

                    return mch.Count;
                }
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        /// <summary>
        /// Unsubscribes the subscription and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed
        /// and unmanaged resources; <c>false</c> to release only unmanaged 
        /// resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                try
                {
                    Unsubscribe();
                }
                catch (Exception)
                {
                    // We we get here with normal usage, for example when
                    // auto unsubscribing, so ignore.
                }

                conn = null;
                closed = true;

                disposedValue = true;
            }
        }

        /// <summary>
        /// Releases all resources used by the <see cref="Subscription"/>.
        /// </summary>
        /// <remarks>This method unsubscribes from the subject, to release resources.</remarks>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion

        /// <summary>
        /// Returns a string that represents the current instance.
        /// </summary>
        /// <returns>A string that represents the current <see cref="Subscription"/>.</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("{");
            
            sb.AppendFormat("Subject={0};Queue={1};" +
                "QueuedMessageCount={2};IsValid={3};Type={4}",
                Subject, (Queue == null ? "null" : Queue), 
                QueuedMessageCount, IsValid, 
                this.GetType().ToString());
            
            sb.Append("}");
            
            return sb.ToString();
        }

        private void checkState()
        {
            if (conn == null || closed)
                throw new NATSBadSubscriptionException();
        }

        /// <summary>
        /// Sets the limits for pending messages and bytes for this instance.
        /// </summary>
        /// <remarks>Zero (<c>0</c>) is not allowed. Negative values indicate that the
        /// given metric is not limited.</remarks>
        /// <param name="messageLimit">The maximum number of pending messages.</param>
        /// <param name="bytesLimit">The maximum number of pending bytes of payload.</param>
        public void SetPendingLimits(long messageLimit, long bytesLimit)
        {
            if (messageLimit == 0)
            {
                throw new ArgumentOutOfRangeException("messageLimit", "The pending message limit must not be zero");
            }
            else if (bytesLimit == 0)
            {
                throw new ArgumentOutOfRangeException("bytesLimit", "The pending bytes limit must not be zero");
            }

            lock (mu)
            {
                checkState();

                pMsgsLimit = messageLimit;
                pBytesLimit = bytesLimit;
            }
        }

        /// <summary>
        /// Gets or sets the maximum allowed count of pending bytes.
        /// </summary>
        /// <value>The limit must not be zero (<c>0</c>). Negative values indicate there is no
        /// limit on the number of pending bytes.</value>
        public long PendingByteLimit 
        { 
            get
            {
                lock (mu)
                {
                    checkState();
                    return pBytesLimit;
                }
            }
            set
            {
                if (value == 0)
                {
                    throw new ArgumentOutOfRangeException("value", "The pending bytes limit must not be zero");
                }

                lock (mu)
                {
                    checkState();
                    pBytesLimit = value;
                }
            }
        }

        /// <summary>
        /// Gets or sets the maximum allowed count of pending messages.
        /// </summary>
        /// <value>The limit must not be zero (<c>0</c>). Negative values indicate there is no
        /// limit on the number of pending messages.</value>
        public long PendingMessageLimit
        {
            get
            {
                lock (mu)
                {
                    checkState();
                    return pMsgsLimit;
                }
            }
            set
            {
                if (value == 0)
                {
                    throw new ArgumentOutOfRangeException("value", "The pending message limit must not be zero");
                }

                lock (mu)
                {
                    checkState();
                    pMsgsLimit = value;
                }
            }
        }

        /// <summary>
        /// Returns the pending byte and message counts.
        /// </summary>
        /// <param name="pendingBytes">When this method returns, <paramref name="pendingBytes"/> will
        /// contain the count of bytes not yet processed on the <see cref="ISubscription"/>.</param>
        /// <param name="pendingMessages">When this method returns, <paramref name="pendingMessages"/> will
        /// contain the count of messages not yet processed on the <see cref="ISubscription"/>.</param>
        public void GetPending(out long pendingBytes, out long pendingMessages)
        {
            lock (mu)
            {
                checkState();
                pendingBytes    = pBytes;
                pendingMessages = pMsgs;
            }
        }

        /// <summary>
        /// Gets the number of bytes not yet processed on this instance.
        /// </summary>
        public long PendingBytes
        {
            get
            {
                lock (mu)
                {
                    checkState();
                    return pBytes;
                }
            }
        }

        /// <summary>
        /// Gets the number of messages not yet processed on this instance.
        /// </summary>
        public long PendingMessages
        {
            get
            {
                lock (mu)
                {
                    checkState();
                    return pMsgs;
                }
            }
        }

        /// <summary>
        /// Returns the maximum number of pending bytes and messages during the life of the <see cref="Subscription"/>.
        /// </summary>
        /// <param name="maxPendingBytes">When this method returns, <paramref name="maxPendingBytes"/>
        /// will contain the current maximum pending bytes.</param>
        /// <param name="maxPendingMessages">When this method returns, <paramref name="maxPendingBytes"/>
        /// will contain the current maximum pending messages.</param>
        public void GetMaxPending(out long maxPendingBytes, out long maxPendingMessages)
        {
            lock (mu)
            {
                checkState();
                maxPendingBytes    = pBytesMax;
                maxPendingMessages = pMsgsMax;
            }
        }

        /// <summary>
        /// Gets the maximum number of pending bytes seen so far by this instance.
        /// </summary>
        public long MaxPendingBytes
        {
            get
            {
                lock (mu)
                {
                    checkState();
                    return pBytesMax;
                }
            }
        }

        /// <summary>
        /// Gets the maximum number of messages seen so far by this instance.
        /// </summary>
        public long MaxPendingMessages
        {
            get
            {
                lock (mu)
                {
                    checkState();
                    return pMsgsMax;
                }
            }
        }

        /// <summary>
        /// Clears the maximum pending bytes and messages statistics.
        /// </summary>
        public void ClearMaxPending()
        {
            lock (mu)
            {
                pMsgsMax = pBytesMax = 0;
            }
        }

        internal Task InternalDrain(int timeout)
        {
            Connection c = null;

            lock (mu)
            {
                if (conn == null || closed)
                    throw new NATSBadSubscriptionException();

                c = conn;
            }

            return c.unsubscribe(this, 0, true, timeout);
        }

        public Task DrainAsync()
        {
            return DrainAsync(Defaults.DefaultDrainTimeout);
        }

        public Task DrainAsync(int timeout)
        {
            if (timeout <= 0)
                throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be greater than zero.");

            return InternalDrain(timeout);
        }

        public void Drain()
        {
           Drain(Defaults.DefaultDrainTimeout);
        }

        public void Drain(int timeout)
        {
            var t = DrainAsync(timeout);
            try
            {
                t.Wait();
            }
            catch (AggregateException)
            {
                throw new NATSTimeoutException();
            }
        }

        /// <summary>
        /// Gets the number of delivered messages for this instance.
        /// </summary>
        public long Delivered
        {
            get
            {
                lock (mu)
                {
                    return delivered;
                }
            }
        }

        /// <summary>
        /// Gets the number of known dropped messages for this instance.
        /// </summary>
        /// <remarks>
        /// This will correspond to the messages dropped by violations of
        /// <see cref="PendingByteLimit"/> and/or <see cref="PendingMessageLimit"/>.
        /// If the NATS server declares the connection a slow consumer, the count
        /// may not be accurate.
        /// </remarks>
        public long Dropped
        {
            get
            {
                lock (mu)
                {
                    return dropped;
                }
            }
        }

        #region validation

        static private readonly char[] invalidSubjectChars = { '\r', '\n', '\t', ' '};

        private static bool ContainsInvalidChars(string value)
        {
            return string.IsNullOrEmpty(value) || value.IndexOfAny(invalidSubjectChars) >= 0;
        }

        /// <summary>
        /// Checks if a subject is valid.
        /// </summary>
        /// <param name="subject">The subject to check</param>
        /// <returns>true if valid, false otherwise.</returns>
        public static bool IsValidSubject(string subject)
        {
            if (ContainsInvalidChars(subject))
            {
                return false;
            }

            // Avoid split for performance, in case this is ever called in the fastpath.
            if (subject.StartsWith(".") || subject.EndsWith(".") || subject.Contains(".."))
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Checks if a prefix is valid.
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static bool IsValidPrefix(string prefix)
        {
            if (ContainsInvalidChars(prefix))
                return false;

            return !prefix.StartsWith(".") && prefix.EndsWith(".");
        }

        /// <summary>
        /// Checks if the queue group name is valid.
        /// </summary>
        /// <param name="queueGroup"></param>
        /// <returns>true is the queue group name is valid, false otherwise.</returns>
        public static bool IsValidQueueGroupName(string queueGroup)
        {
            return ContainsInvalidChars(queueGroup) == false;
        }

        #endregion

    }  // Subscription

}