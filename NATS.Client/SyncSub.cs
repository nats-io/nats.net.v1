// Copyright 2015-2017 Apcera Inc. All rights reserved.

// disable XM comment warnings
#pragma warning disable 1591

using System;

namespace NATS.Client
{
    public sealed class SyncSubscription : Subscription, ISyncSubscription, ISubscription 
    {
        internal SyncSubscription(Connection conn, string subject, string queue)
            : base(conn, subject, queue)
        {
            mch = new Channel<Msg>()
            {
                Name = subject + (String.IsNullOrWhiteSpace(queue) ? "" : " (queue: " + queue + ")"),
            };
        }

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
                long d;
                lock (mu)
                {
                    d = tallyDeliveredMessage(msg);
                }
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