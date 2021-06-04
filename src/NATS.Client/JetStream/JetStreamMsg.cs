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
using System.Text;

namespace NATS.Client.JetStream
{
    public sealed class JetStreamMsg : Msg
    {
        static readonly private byte[] ackAck = Encoding.ASCII.GetBytes("+ACK");
	    static readonly private byte[] ackNak = Encoding.ASCII.GetBytes("-NAK");
	    static readonly private byte[] ackProgress = Encoding.ASCII.GetBytes("+WPI");
	    static readonly private byte[] ackTerm = Encoding.ASCII.GetBytes("+TERM");

        /// <summary>
        /// Gets the metadata associated with a JetStream message.
        /// </summary>
        public MetaData MetaData { get; }


        private IConnection Connection { get; }

        // fastpath check to see if this is a valid JS message.
        // Speed is of the essence here.
        static internal bool IsJS(string replySubj)
        {
            if (replySubj == null)
                return false;

            int len = replySubj.Length;
            if (len < 9)
            {
                // quick and dirty check
                return false;
            }

            if (replySubj[0] != '$' ||
                replySubj[1] != 'J' ||
                replySubj[2] != 'S' ||
                replySubj[3] != '.' ||
                replySubj[4] != 'A' ||
                replySubj[5] != 'C' ||
                replySubj[6] != 'K' ||
                replySubj[7] != '.')
            {
                return false;
            }

            int i;
            for (i = 8; i < len; i++)
            {
                if (replySubj[i] == '.')
                {
                    i++;
                }
            }
            return i == 7;
        }

        // Take the reply and parse it into the metadata.
        internal JetStreamMsg(IConnection conn, string subject, string reply, byte[]data) : base(subject, reply, data)
        {
            Connection = conn;
            MetaData = new MetaData(reply);
        }

        internal JetStream CheckReply(out bool isPullMode)
        {
            // TODO: Implement;
            if ((sub is JetStreamSubscription) == false)
            {
                // Not a Jetstream enabled connection.
                isPullMode = false;
                return null;
            }

            isPullMode = (sub is JetStreamPullSubscription);
            return ((JetStreamSubscription)sub).JS;
        }

        private void AckReply(byte[] ackType, int timeout)
        {
            if (timeout > 0)
            {
                Connection.Request(Reply, ackType, timeout);
            }
            else
            {     
                Connection.Publish(Reply, ackType);
            }
        }

        private void AckReply(byte[] ackType) => AckReply(ackType, 0);


        /// <summary>
        /// Acknowledges a JetStream messages received from a Consumer,
        /// indicating the message will not be resent.
        /// </summary>
        public override void Ack()
        {
            AckReply(ackAck);
        }

        /// <summary>
        /// Acknowledges a JetStream messages received from a Consumer,
        /// indicating the message should not be received again later.
        /// A timeout of zero does not confirm the acknowledgement.
        /// </summary>
        /// <param name="timeout">the duration to wait for an ack in milliseconds
        /// confirmation</param>
        public override void AckSync(int timeout) =>  AckReply(ackAck, timeout);
        

        /// <summary>
        /// Acknowledges a JetStream message has been received but indicates
        /// that the message is not completely processed and should be sent
        /// again later.
        /// </summary>
        public override void Nak() => AckReply(ackNak);

        /// <summary>
        /// Prevents this message from ever being delivered regardless of
        /// maxDeliverCount.
        /// </summary>
        public override void Term() => AckReply(ackTerm);

        /// <summary>
        /// Indicates that this message is being worked on and reset redelivery timer in the server.
        /// </summary>
        public override void InProgress() => AckReply(ackProgress);

        /// <summary>
        /// Checks if a message is from Jetstream or is a standard message.
        /// </summary>
        /// <returns></returns>
        public override bool IsJetStream { get { return true; } }
    }

    /// <summary>
    /// Represents a pair of sequence numbers held by a stream and consumer.
    /// </summary>
    public sealed class Sequence
    {
        /// <summary>
        /// Gets the Stream sequence number.
        /// </summary>
        public ulong StreamSequence { get; }

        /// <summary>
        /// Gets the Consumer sequence number.
        /// </summary>
        public ulong ConsumerSequence { get; }

        internal Sequence(ulong streamSeq, ulong consSeq)
        {
            StreamSequence = streamSeq;
            ConsumerSequence = consSeq;
        }

    }

    /// <summary>
    /// JetStream essage MetaData
    /// </summary>
    public sealed class MetaData
    {
        static readonly DateTime epochTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Number of delivered messages
        /// </summary>
        public ulong NumDelivered { get; }

        /// <summary>
        /// Number of pending messages
        /// </summary>
        public ulong NumPending { get;  }

        /// <summary>
        /// Gets the timestamp of the message.
        /// </summary>
        public DateTime Timestamp { get; }

        /// <summary>
        /// Gets the raw nanosecond timestamp of the message.
        /// </summary>
        public ulong TimestampNanos { get; }

        /// <summary>
        /// Gets the stream name.
        /// </summary>
        public string Stream { get;  }

        /// <summary>
        /// Gets the consumer name.
        /// </summary>
        public string Consumer { get;  }

        /// <summary>
        /// Gets the consumer sequence number of the message.
        /// </summary>
        public Sequence Sequence { get; }


        // Caller must ensure this is a JS message
        internal MetaData(string metaData)
        {
            string[] tokens = metaData?.Split('.');
            if (tokens.Length != 9)
            {
                throw new NATSException($"Invalid MetaData: {metaData}");
            }

            Stream = tokens[2];
            Consumer = tokens[3];
            NumDelivered = ulong.Parse(tokens[4]);
            Sequence = new Sequence(ulong.Parse(tokens[5]), ulong.Parse(tokens[6]));
            TimestampNanos = ulong.Parse(tokens[6]);
            Timestamp = epochTime.AddTicks((long)TimestampNanos/100);
        }

    }
}
