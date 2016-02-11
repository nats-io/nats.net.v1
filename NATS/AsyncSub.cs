// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Threading;
using System.Threading.Tasks;

// disable XML comment warnings
#pragma warning disable 1591

namespace NATS.Client
{
    public sealed class AsyncSubscription : Subscription, IAsyncSubscription, ISubscription
    {
        public event EventHandler<MsgHandlerEventArgs> MessageHandler;

        private MsgHandlerEventArgs msgHandlerArgs = new MsgHandlerEventArgs();
        private Task                msgFeeder = null;

        internal AsyncSubscription(Connection conn, string subject, string queue)
            : base(conn, subject, queue) { }

        internal AsyncSubscription(Connection conn, string subject, string queue,
            EventHandler<MsgHandlerEventArgs> messageEventHandler) : base(conn, subject, queue)
        {
            MessageHandler = messageEventHandler;
            Start();
        }

        internal protected override bool processMsg(Msg msg)
        {
            Connection localConn;
            EventHandler<MsgHandlerEventArgs> localHandler;
            long localMax;

            lock (mu)
            {
                localConn = this.conn;
                localHandler = MessageHandler;
                localMax = this.max;
                if (this.closed)
                    return false;
            }

            // the message handler has not been setup yet, drop the 
            // message.
            if (MessageHandler == null)
                return true;

            if (conn == null)
                return false;

            long d = tallyDeliveredMessage(msg);
            if (localMax <= 0 || d <= localMax)
            {
                msgHandlerArgs.msg = msg;
                try
                {
                    localHandler(this, msgHandlerArgs);
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

        public void Start()
        {
            if (isStarted())
                return;

            if (conn == null)
                throw new NATSBadSubscriptionException();

            conn.sendSubscriptionMessage(this);
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
