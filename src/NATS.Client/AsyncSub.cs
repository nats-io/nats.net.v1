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
using System.Threading;
using System.Threading.Tasks;

// disable XML comment warnings
#pragma warning disable 1591

namespace NATS.Client
{
    /// <summary>
    /// <see cref="AsyncSubscription"/> asynchronously delivers messages to listeners of the <see cref="MessageHandler"/>
    /// event. This class should not be used directly.
    /// </summary>
    /// <remarks>
    /// If the <see cref="AsyncSubscription"/> is created without listening to the <see cref="MessageHandler"/>
    /// event, no messages will be received until <see cref="Start()"/> has been called.
    /// </remarks>
    public class AsyncSubscription : Subscription, IAsyncSubscription, ISubscription
    {
        /// <summary>
        /// Occurs when the <see cref="AsyncSubscription"/> receives a message from the
        /// underlying <see cref="Subscription"/>.
        /// </summary>
        public event EventHandler<MsgHandlerEventArgs> MessageHandler;

        private Task msgFeeder = null;

        private bool started = false;

        internal AsyncSubscription(Connection conn, string subject, string queue)
            : base(conn, subject, queue)
        {
            mch = conn.getMessageChannel();
            if ((ownsChannel = (mch == null)))
            {
                mch = new Channel<Msg>()
                {
                    Name = subject + (String.IsNullOrWhiteSpace(queue) ? "" : " (queue: " + queue + ")"),
                };
            }
        }

        internal override bool processMsg(Msg msg)
        {
            Connection localConn;
            EventHandler<MsgHandlerEventArgs> localHandler;
            long localMax;
            long d;

            lock (mu)
            {
                if (closed)
                    return false;

                // the message handler has not been setup yet, drop the 
                // message.
                if (MessageHandler == null)
                    return true;

                if (conn == null)
                    return false;

                d = tallyDeliveredMessage(msg);

                localConn = conn;
                localHandler = MessageHandler;
                localMax = max;
            }

            if (localMax <= 0 || d <= localMax)
            {
                try
                {
                    if (localHandler != null)
                    {
                        var msgHandlerEventArgs = new MsgHandlerEventArgs();
                        msgHandlerEventArgs.msg = msg;

                        localHandler(this, msgHandlerEventArgs);
                    }
                }
                catch (Exception) { }

                if (d == max)
                {
                    unsubscribe(false);
                    lock (mu)
                    {
                        conn = null;
                    }
                }
            }

            return true;
        }

        internal bool isStarted()
        {
            return started;
        }

        internal void enableAsyncProcessing()
        {
            if (ownsChannel && msgFeeder == null)
            {
                // Use the default task scheduler and do not let child tasks launched
                // when delivering messages to attach to this task (Issue #273)
                msgFeeder = Task.Factory.StartNew(
                    doAsyncProcessing,
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                    TaskScheduler.Default);
            }
            started = true;
        }

        private void doAsyncProcessing() => conn.deliverMsgs(mch);

        internal void disableAsyncProcessing()
        {
            lock (mu)
            {
                if (msgFeeder != null)
                {
                    mch.close();
                    msgFeeder = null;
                }
                MessageHandler = null;
                started = false;
            }
        }

        /// <summary>
        /// Starts delivering received messages to listeners on <see cref="MessageHandler"/>
        /// from a separate thread.
        /// </summary>
        /// <remarks>
        /// If the <see cref="IAsyncSubscription"/> has already started delivering messages, this
        /// method is a no-op.
        /// </remarks>
        /// <exception cref="NATSBadSubscriptionException">There is no longer an associated <see cref="Connection"/>
        /// for this <see cref="AsyncSubscription"/>.</exception>
        public void Start()
        {
            if (started)
                return;

            lock (mu)
            {
                if (conn == null)
                    throw new NATSBadSubscriptionException();

                conn.sendSubscriptionMessage(this);
                enableAsyncProcessing();
            }
        }

        /// <summary>
        /// Removes interest in the given subject.
        /// </summary>
        /// <exception cref="NATSBadSubscriptionException">There is no longer an associated <see cref="Connection"/>
        /// for this <see cref="AsyncSubscription"/>.</exception>
        public override void Unsubscribe()
        {
            disableAsyncProcessing();
            base.Unsubscribe();
        }

        /// <summary>
        /// Issues an automatic call to <see cref="Unsubscribe"/> when <paramref name="max"/> messages have been
        /// received.
        /// </summary>
        /// <remarks><para>This can be useful when sending a request to an unknown number of subscribers.
        /// <see cref="Connection"/>'s Request methods use this functionality.</para>
        /// <para>Calling this method will invoke <see cref="Start"/> if it has not already been called.</para></remarks>
        /// <param name="max">The maximum number of messages to receive on the subscription before calling
        /// <see cref="Unsubscribe"/>. Values less than or equal to zero (<c>0</c>) unsubscribe immediately.</param>
        /// <exception cref="NATSBadSubscriptionException">There is no longer an associated <see cref="Connection"/>
        /// for this <see cref="AsyncSubscription"/>.</exception>
        public override void AutoUnsubscribe(int max)
        {
            Start();
            base.AutoUnsubscribe(max);
        }

        internal override void close()
        {
            disableAsyncProcessing();
            close(ownsChannel);
        }
    }
}
