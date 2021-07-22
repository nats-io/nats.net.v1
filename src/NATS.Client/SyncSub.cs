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

namespace NATS.Client
{
    /// <summary>
    /// <see cref="SyncSubscription"/> provides messages for a subject through calls
    /// to <see cref="NextMessage()"/> and <see cref="NextMessage(int)"/>. This class should
    /// not be used directly.
    /// </summary>
    public class SyncSubscription : Subscription, ISyncSubscription, ISubscription 
    {
        internal SyncSubscription(Connection conn, string subject, string queue)
            : base(conn, subject, queue)
        {
            mch = new Channel<Msg>()
            {
                Name = subject + (String.IsNullOrWhiteSpace(queue) ? "" : " (queue: " + queue + ")"),
            };
        }

        /// <summary>
        /// Returns the next <see cref="Msg"/> available to a synchronous
        /// subscriber, blocking until one is available.
        /// </summary>
        /// <returns>The next <see cref="Msg"/> available to a subscriber.</returns>
        /// <exception cref="NATSConnectionClosedException">The connection to the NATS Server
        /// is closed.</exception>
        /// <exception cref="NATSMaxMessagesException">The maximum number of messages have been
        /// delivered to this <see cref="ISyncSubscription"/>.</exception>
        /// <exception cref="NATSBadSubscriptionException">The subscription is closed.</exception>
        /// <exception cref="NATSSlowConsumerException">The subscription has been marked as a slow consumer.</exception>
        public Msg NextMessage()
        {
            return NextMessage(-1);
        }

        /// <summary>
        /// Returns the next <see cref="Msg"/> available to a synchronous
        /// subscriber, or block up to a given timeout until the next one is available.
        /// </summary>
        /// <param name="timeout">The amount of time, in milliseconds, to wait for
        /// the next message.</param>
        /// <returns>The next <see cref="Msg"/> available to a subscriber.</returns>
        /// <exception cref="NATSConnectionClosedException">The connection to the NATS Server
        /// is closed.</exception>
        /// <exception cref="NATSMaxMessagesException">The maximum number of messages have been
        /// delivered to this <see cref="ISyncSubscription"/>.</exception>
        /// <exception cref="NATSBadSubscriptionException">The subscription is closed.</exception>
        /// <exception cref="NATSSlowConsumerException">The subscription has been marked as a slow consumer.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while waiting for the next available
        /// <see cref="Msg"/>.</exception>
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
                if (d == localMax)
                {
                    // Remove subscription if we have reached max.
                    localConn.removeSubSafe(this);
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