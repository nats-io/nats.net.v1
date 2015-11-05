// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Threading;
using System.Threading.Tasks;

// disable XML comment warnings
#pragma warning disable 1591

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
    public sealed class AsyncSubscription : Subscription, IAsyncSubscription, ISubscription
    {

        public MsgHandler          msgHandler = null;
        private MsgHandlerEventArgs msgHandlerArgs = new MsgHandlerEventArgs();
        private Task                msgFeeder = null;

        internal AsyncSubscription(Connection conn, string subject, string queue)
            : base(conn, subject, queue) { }

        internal protected override bool processMsg(Msg msg)
        {
            Connection c;
            MsgHandler handler;
            long max;

            lock (mu)
            {
                c = this.conn;
                handler = this.msgHandler;
                max = this.max;
            }

            // the message handler has not been setup yet, drop the 
            // message.
            if (msgHandler == null)
                return true;

            if (conn == null)
                return false;

            long d = Interlocked.Increment(ref delivered);
            if (max <= 0 || d <= max)
            {
                msgHandlerArgs.msg = msg;
                try
                {
                    msgHandler(this, msgHandlerArgs);
                }
                catch (Exception) { }

                if (d == max)
                {
                    Unsubscribe();
                    this.conn = null;
                }
            }

            return true;
        }

        internal bool isStarted()
        {
            return (msgFeeder != null);
        }

        internal void enableAsyncProcessing()
        {
            if (msgFeeder == null)
            {
                msgFeeder = new Task(() => { conn.deliverMsgs(mch); });
                msgFeeder.Start();
            }
        }

        internal void disableAsyncProcessing()
        {
            if (msgFeeder != null)
            {
                mch.close();               
                msgFeeder = null;
            }
        }

        /// <summary>
        /// Adds or removes a message handler to this subscriber.
        /// </summary>
        /// <remarks><see cref="MsgHandler">See MsgHandler</see></remarks>
        public event MsgHandler MessageHandler
        {
            add
            {
                msgHandler += value;
            }
            remove
            {
                msgHandler -= value;
            }
        }

        /// <summary>
        /// This completes the subsciption process notifying the server this subscriber
        /// has interest.
        /// </summary>
        public void Start()
        {
            if (isStarted())
                return;

            if (conn == null)
                throw new NATSBadSubscriptionException();

            conn.sendSubscriptonMessage(this);
            enableAsyncProcessing();
        }

        override public void Unsubscribe()
        {
            disableAsyncProcessing();
            base.Unsubscribe();
        }

        public override void AutoUnsubscribe(int max)
        {
            if (!isStarted())
                Start();

            base.AutoUnsubscribe(max);
        }
    }
}