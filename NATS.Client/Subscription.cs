// Copyright 2015-2017 Apcera Inc. All rights reserved.

using System;
using System.Text;

// disable XML comment warnings
#pragma warning disable 1591

namespace NATS.Client
{
    public class Subscription : ISubscription, IDisposable
    {
        readonly  internal  object mu = new object(); // lock

        internal  long           sid = 0; // subscriber ID.
        private   long           msgs;
        internal  protected long delivered;
        private   long           bytes;
        internal  protected long max = -1;

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

        public string Subject
        {
            get { return subject; }
        }

        // Optional queue group name. If present, all subscriptions with the
        // same name will form a distributed queue, and each message will
        // only be processed by one member of the group.
        string queue;

        public string Queue
        {
            get { return queue; }
        }

        public Connection Connection
        {
            get
            {
                return conn;
            }
        }
 
        internal bool tallyMessage(long bytes)
        {
            lock (mu)
            {
                if (max > 0 && msgs > max)
                    return true;

                this.msgs++;
                this.bytes += bytes;

            }

            return false;
        }


        protected internal virtual bool processMsg(Msg msg)
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

        // caller must lock
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

        public bool IsValid
        {
            get
            {
                lock (mu)
                {
                    return (conn != null);
                }
            }
        }

        internal void unsubscribe(bool throwEx)
        {
            Connection c;
            lock (mu)
            {
                c = this.conn;
            }

            if (c == null)
            {
                if (throwEx)
                    throw new NATSBadSubscriptionException();

                return;
            }

            c.unsubscribe(this, 0);
        }

        public virtual void Unsubscribe()
        {
            unsubscribe(true);
        }

        public virtual void AutoUnsubscribe(int max)
        {
            Connection c = null;

            lock (mu)
            {
                if (conn == null)
                    throw new NATSBadSubscriptionException();

                c = conn;
            }

            c.unsubscribe(this, max);
        }

        /// <summary>
        /// Gets the number of messages remaining in the delivery queue.
        /// </summary>
        /// <exception cref="NATSBadSubscriptionException">There is no longer an associated <see cref="Connection"/>
        /// for this <see cref="AsyncSubscription"/>.</exception>
        public int QueuedMessageCount
        {
            get
            {
                lock (mu)
                {
                    if (conn == null)
                        throw new NATSBadSubscriptionException();

                    return mch.Count;
                }
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

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

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion

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
            if (conn == null)
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
        /// contain the count of bytes not yet processed on the <see cref="Subscription"/>.</param>
        /// <param name="pendingMessages">When this method returns, <paramref name="pendingMessages"/> will
        /// contain the count of messages not yet processed on the <see cref="Subscription"/>.</param>
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

    }  // Subscription

}