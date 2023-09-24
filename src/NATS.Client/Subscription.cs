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
using NATS.Client.Internals;

// disable XML comment warnings
#pragma warning disable 1591

namespace NATS.Client
{
    internal class SidGenerator
    {
        private static readonly InterlockedLong Generator = new InterlockedLong();
        internal static long Next() => Generator.Increment();
    }
    /// <summary>
    /// Represents interest in a NATS topic. This class should
    /// not be used directly.
    /// </summary>
    public class Subscription : ISubscription, IDisposable
    {

        internal  readonly  object mu = new object(); // lock

        internal  long           sid; // subscriber ID.
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
        internal long pendingMessages = 0;
        internal long pendingBytes = 0;
        internal long pendingMessagesMax = 0;
        internal long pendingBytesMax = 0;
        internal long pendingMessagesLimit; // getting this from the options
        internal long pendingBytesLimit = Defaults.SubPendingBytesLimit;
        internal long dropped = 0;

        // Subject that represents this subscription. This can be different
        // than the received subject inside a Msg if this is a wildcard.
        private string subject = null;

        internal Func<Msg, bool> _beforeChannelAddCheck;
        internal Func<Msg, bool> BeforeChannelAddCheck
        {
            get => _beforeChannelAddCheck;
            set
            {
                if (value == null)
                {
                    _beforeChannelAddCheck = m => true;
                }
                else
                {
                    _beforeChannelAddCheck = value;
                }
            }
        }

        internal Subscription(Connection conn, string subject, string queue)
        {
            this.conn = conn;
            this.subject = subject;
            this.queue = queue;

            pendingMessagesLimit = conn.Opts.subChanLen;
            
            sid = SidGenerator.Next();

            BeforeChannelAddCheck = null;
        }
                
        internal void ReSubscribe(string deliverSubject)
        {
            conn.SendUnsub(sid, 0);
            conn.RemoveSubscription(this);
            sid = SidGenerator.Next();
            conn.AddSubscription(this);
            conn.SendSub(deliverSubject, queue, sid);
            subject = deliverSubject;
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
        /// the id associated with the subscription, used by the connection when processing an incoming
        /// </summary>
        public long Sid => sid; 

        /// <summary>
        /// Gets the subject for this subscription.
        /// </summary>
        public string Subject => subject;

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
        public string Queue => queue;

        internal string SubName() => subject + "(" + sid + ")" +
                                (string.IsNullOrWhiteSpace(queue) ? "" : " (queue: " + queue + ")");


        /// <summary>
        /// Gets the <see cref="Connection"/> associated with this instance.
        /// </summary>
        public Connection Connection => conn;

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
            pendingMessages--;
            pendingBytes -= msg.Data.Length;
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
            pendingBytes -= msg.Data.Length;
            pendingMessages--;

            return delivered;
        }

        // returns false if the message could not be added because
        // the channel is full, true if the message was added
        // to the channel.
        internal bool addMessage(Msg msg)
        {
            // Subscription internal stats
	        pendingMessages++;
	        if (pendingMessages > pendingMessagesMax)
            {
		        pendingMessagesMax = pendingMessages;
            }
	
	        pendingBytes += msg.Data.Length;
	        if (pendingBytes > pendingBytesMax)
            {
		        pendingBytesMax = pendingBytes;
            }
	
            // BeforeChannelAddCheck returns true if the message is allowed to be queued
            if (BeforeChannelAddCheck.Invoke(msg))
            {
                // Check for a Slow Consumer
                if ((pendingMessagesLimit > 0 && pendingMessages > pendingMessagesLimit)
                    || (pendingBytesLimit > 0 && pendingBytes > pendingBytesLimit))
                {
                    // slow consumer
                    handleSlowConsumer(msg);
                    return false;
                }
                sc = false;
                mch?.add(msg);
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
        /// Any value less than or equal to zero means unlimited and will be stored as -1.
        /// </summary>
        /// <param name="messageLimit">The maximum number of pending messages.</param>
        /// <param name="bytesLimit">The maximum number of pending bytes of payload.</param>
        public void SetPendingLimits(long messageLimit, long bytesLimit)
        {
            lock (mu)
            {
                checkState();
                SetPendingMessageLimitInternal(messageLimit);
                SetPendingByteLimitInternal(bytesLimit);
            }
        }

        private void SetPendingByteLimitInternal(long bytesLimit)
        {
            pendingBytesLimit = bytesLimit <= 0 ? -1 : bytesLimit;
        }

        private void SetPendingMessageLimitInternal(long messageLimit)
        {
            pendingMessagesLimit = messageLimit <= 0 ? -1 : messageLimit;
        }

        /// <summary>
        /// Gets or sets the maximum allowed count of pending bytes.
        /// </summary>
        /// <value>The pending byte limit if greater than 0 or -1 for unlimited.</value>
        public long PendingByteLimit 
        { 
            get
            {
                lock (mu)
                {
                    checkState();
                    return pendingBytesLimit;
                }
            }
            set
            {
                lock (mu)
                {
                    checkState();
                    SetPendingByteLimitInternal(value);
                }
            }
        }

        /// <summary>
        /// Gets or sets the maximum allowed count of pending messages.
        /// </summary>
        /// <value>The pending message limit if greater than 0 or -1 for unlimited.</value>
        public long PendingMessageLimit
        {
            get
            {
                lock (mu)
                {
                    checkState();
                    return pendingMessagesLimit;
                }
            }
            set
            {
                lock (mu)
                {
                    checkState();
                    SetPendingMessageLimitInternal(value);
                }
            }
        }

        /// <summary>
        /// Returns the yet processed pending byte and message counts.
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
                pendingBytes    = this.pendingBytes;
                pendingMessages = this.pendingMessages;
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
                    return pendingBytes;
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
                    return pendingMessages;
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
                maxPendingBytes    = pendingBytesMax;
                maxPendingMessages = pendingMessagesMax;
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
                    return pendingBytesMax;
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
                    return pendingMessagesMax;
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
                pendingMessagesMax = pendingBytesMax = 0;
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

        private static readonly char[] invalidSubjectChars = { '\r', '\n', '\t', ' '};

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
            return Validator.IsValidSubjectTerm(subject, "subject", true).Item1;
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
            return Validator.IsValidSubjectTerm(queueGroup, "queueGroup", true).Item1;
        }

        #endregion

    }  // Subscription

}