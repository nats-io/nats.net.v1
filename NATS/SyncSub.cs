// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Threading;
using System.Threading.Tasks;

// disable XM comment warnings
#pragma warning disable 1591

namespace NATS.Client
{
    public sealed class SyncSubscription : Subscription, ISyncSubscription, ISubscription 
    {
        internal SyncSubscription(Connection conn, string subject, string queue)
            : base(conn, subject, queue) { }

        public Msg NextMessage()
        {
            return NextMessage(-1);
        }

        public Msg NextMessage(int timeout)
        {
            Connection   localConn;
            Channel<Msg> localChannel;
            long         localMax;
            Msg          msg;

            lock (mu)
            {
                if (connClosed)
                {
                    throw new NATSConnectionClosedException();
                }
                else if (max > 0 && delivered >= max)
                {
                    throw new NATSMaxMessagesException();
                }
                else if (closed)
                {
                    throw new NATSBadSubscriptionException();
                }
                if (sc)
                {
                    sc = false;
                    throw new NATSSlowConsumerException();
                }

                localConn = this.conn;
                localChannel = this.mch;
                localMax = this.max;
            }

            if (localMax > 0 && this.delivered >= localMax)
            {
                throw new NATSMaxMessagesException();
            }

            if (timeout >= 0)
            {
                msg = localChannel.get(timeout);
            }
            else
            {
                msg = localChannel.get(-1);
            }

            if (msg != null)
            {
                long d = tallyDeliveredMessage(msg);
                if (d == max)
                {
                    // Remove subscription if we have reached max.
                    localConn.removeSub(this);
                }
                if (localMax > 0 && d > localMax)
                {
                    throw new NATSMaxMessagesException();
                }
            }

            return msg;
        }
    }
}