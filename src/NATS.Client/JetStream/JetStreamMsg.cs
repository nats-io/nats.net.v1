// Copyright 2021 The NATS Authors
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
using NATS.Client.Internals;
using static NATS.Client.AckType;

namespace NATS.Client.JetStream
{
    public sealed class JetStreamMsg : Msg
    {
        /// <summary>
        /// Gets the metadata associated with a JetStream message.
        /// </summary>
        public override MetaData MetaData { get; }

        private IConnection Connection { get; }

        // Take the reply and parse it into the metadata.
        internal JetStreamMsg(IConnection conn, MsgArg arg, Subscription s, byte[] payload, long totalLen) : 
            base(arg, s, payload, totalLen)
        {
            Connection = conn;
            MetaData = new MetaData(_reply);
        }

        private void AckReply(AckType ackType, long delayNanoseconds, int timeout)
        {
            if (AckHasntBeenTermed())
            {
                // very important, must use _reply variable, not public Reply property
                if (timeout >= 0)
                {
                    Connection.Request(_reply, ackType.BodyBytes(delayNanoseconds), timeout);
                }
                else
                {
                    Connection.Publish(_reply, ackType.BodyBytes(delayNanoseconds));
                }

                _lastAck = ackType;
            }
        }

        private bool AckHasntBeenTermed() {
            return _lastAck == null || !_lastAck.IsTerminal;
        }

        /// <summary>
        /// Acknowledges a JetStream messages received from a Consumer,
        /// indicating the message will not be resent.
        /// </summary>
        public override void Ack() => AckReply(AckAck, -1, -1);

        /// <summary>
        /// Acknowledges a JetStream messages received from a Consumer,
        /// indicating the message should not be received again later.
        /// A timeout of zero does not confirm the acknowledgement.
        /// </summary>
        /// <param name="timeout">the duration to wait for an ack in milliseconds
        /// confirmation</param>
        public override void AckSync(int timeout) => AckReply(AckAck, -1, timeout);
        
        /// <summary>
        /// Acknowledges a JetStream message has been received but indicates
        /// that the message is not completely processed and should be sent
        /// again later.
        /// </summary>
        public override void Nak() => AckReply(AckNak, -1, -1);

        /// <summary>
        /// Acknowledges a JetStream message has been received but indicates
        /// that the message is not completely processed and should be sent
        /// again later, after at least the delay amount.
        /// </summary>
        public override void NakWithDelay(Duration nakDelay) => AckReply(AckNak, nakDelay.Nanos, -1);

        /// <summary>
        /// Acknowledges a JetStream message has been received but indicates
        /// that the message is not completely processed and should be sent
        /// again later, after at least the delay amount.
        /// </summary>
        public override void NakWithDelay(long nakDelayMillis) => AckReply(AckNak, nakDelayMillis * Duration.NanosPerMilli, -1);

        /// <summary>
        /// Prevents this message from ever being delivered regardless of
        /// maxDeliverCount.
        /// </summary>
        public override void Term() => AckReply(AckTerm, -1, -1);

        /// <summary>
        /// Indicates that this message is being worked on and reset redelivery timer in the server.
        /// </summary>
        public override void InProgress() => AckReply(AckProgress, -1, -1);

        /// <summary>
        /// Checks if a message is from JetStream or is a standard message.
        /// </summary>
        /// <returns></returns>
        public override bool IsJetStream => true;

        /// <summary>
        /// A JetStream message does not have a reply that is presented
        /// to the application.
        /// </summary>
        public new string Reply => null;
    }

    /// <summary>
    /// JetStream message MetaData
    /// </summary>
    public sealed class MetaData
    {
        static readonly DateTime epochTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Gets the prefix.
        /// </summary>
        public string Prefix { get;  }

        /// <summary>
        /// Gets the stream name.
        /// </summary>
        public string Stream { get;  }

        /// <summary>
        /// Gets the domain name.
        /// </summary>
        public string Domain { get;  }

        /// <summary>
        /// Gets the consumer name.
        /// </summary>
        public string Consumer { get;  }

        /// <summary>
        /// Number of delivered messages
        /// </summary>
        public ulong NumDelivered { get; }

        /// <summary>
        /// Gets the Stream sequence number.
        /// </summary>
        public ulong StreamSequence { get; }

        /// <summary>
        /// Gets the Consumer sequence number.
        /// </summary>
        public ulong ConsumerSequence { get; }

        /// <summary>
        /// Gets the timestamp of the message.
        /// </summary>
        public DateTime Timestamp { get; }

        /// <summary>
        /// Gets the raw nanosecond timestamp of the message.
        /// </summary>
        public ulong TimestampNanos { get; }

        /// <summary>
        /// Number of pending messages
        /// </summary>
        public ulong NumPending { get;  }

        internal string AccountHash { get;  }

        // Caller must ensure this is a JS message
        internal MetaData(string metaData)
        {
            string[] parts = metaData?.Split('.');
            if (parts == null || parts.Length < 8 || !"ACK".Equals(parts?[1]))
            {
                throw new NATSException($"Invalid MetaData: {metaData}");
            }
            
            int streamIndex;
            bool hasPending;
            bool hasDomainAndHash;
            if (parts.Length == 8)
            {
                streamIndex = 2;
                hasPending = false;
                hasDomainAndHash = false;
            }
            else if (parts.Length == 9)
            {
                streamIndex = 2;
                hasPending = true;
                hasDomainAndHash = false;
            }
            else if (parts.Length >= 11)
            {
                streamIndex = 4;
                hasPending = true;
                hasDomainAndHash = true;
            }
            else
            {
                throw new NATSException($"Invalid MetaData: {metaData}");
            }

            try
            {
                Prefix = parts[0];
                // "ack" = parts[1]
                Domain = hasDomainAndHash ? parts[2] : null;
                AccountHash = hasDomainAndHash ? parts[3] : null;
                Stream = parts[streamIndex];
                Consumer = parts[streamIndex + 1];
                NumDelivered = ulong.Parse(parts[streamIndex + 2]);
                StreamSequence = ulong.Parse(parts[streamIndex + 3]);
                ConsumerSequence = ulong.Parse(parts[streamIndex + 4]);

                TimestampNanos = ulong.Parse(parts[streamIndex + 5]);
                Timestamp = epochTime.AddTicks((long)TimestampNanos / 100);

                NumPending = hasPending ? ulong.Parse(parts[streamIndex + 6]) : 0;
            }
            catch (Exception)
            {
                throw new NATSException($"Invalid MetaData: {metaData}");
            }
        }

        public override string ToString()
        {
            return $"Stream={Stream}, Consumer={Consumer}, NumDelivered={NumDelivered}, StreamSequence={StreamSequence}, ConsumerSequence={ConsumerSequence}, Timestamp={Timestamp}, NumPending={NumPending}";
        }
    }
}
