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

        private bool started = false;

        internal AsyncSubscription(Connection conn, string subject, string queue)
            : base(conn, subject, queue)
        {
            mch = conn.getMessageChannel();
            if ((ownsChannel = (mch == null)))
            {
                mch = new Channel<Msg>();
            }
        }

        internal protected override bool processMsg(Msg msg)
        {
            Connection localConn;
            EventHandler<MsgHandlerEventArgs> localHandler;
            long localMax;

            lock (mu)
            {
                if (closed)
                    return false;

                localConn = conn;
                localHandler = MessageHandler;
                localMax = max;
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
                    unsubscribe(false);
                    conn = null;
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
                msgFeeder = new Task(() => { conn.deliverMsgs(mch); });
                msgFeeder.Start();
            }
            started = true;
        }

        internal void disableAsyncProcessing()
        {
            if (msgFeeder != null)
            {
                mch.close();               
                msgFeeder = null;
            }
            started = false;
        }

        public void Start()
        {
            if (started)
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
            Start();
            base.AutoUnsubscribe(max);
        }

        internal override void close()
        {
            close(ownsChannel);
        }
    }
}
