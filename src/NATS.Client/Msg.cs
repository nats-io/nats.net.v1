// Copyright 2015-2020 The NATS Authors
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
using NATS.Client.Internals;

namespace NATS.Client
{
    /// <summary>
    /// A NATS message is an object encapsulating a subject, optional reply
    /// payload, optional header, and subscription information, sent or
    /// received by the client application.
    /// </summary>
    public class Msg
    {
        protected static readonly byte[] Empty = new byte[0];
        protected string subject;
        protected string _reply;
        protected byte[] data;
        internal Subscription sub;
        internal MsgHeader header;
        internal MsgStatus status;

        /// <summary>
        /// Initializes a new instance of the <see cref="Msg"/> class without any
        /// subject, reply, or data.
        /// </summary>
        public Msg()
        {
            subject = null;
            _reply = null;
            data = null;
            sub = null;
            header = null;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Msg"/> class with a subject, reply, header, and data.
        /// </summary>
        /// <param name="subject">Subject of the message.</param>
        /// <param name="reply">A reply subject, or <c>null</c>.</param>
        /// <param name="header">Message headers or <c>null</c>.</param>
        /// <param name="data">A byte array containing the message payload.</param>
        public Msg(string subject, string reply, MsgHeader header, byte[] data)
        {
            if (string.IsNullOrWhiteSpace(subject))
            {
                throw new ArgumentException(
                    "Subject cannot be null, empty, or whitespace.",
                    "subject");
            }

            this.Subject = subject;
            this.Reply = reply;
            this.Header = header;
            this.Data = data;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Msg"/> class with a subject, reply, and data.
        /// </summary>
        /// <param name="subject">Subject of the message.</param>
        /// <param name="reply">A reply subject, or <c>null</c>.</param>
        /// <param name="data">A byte array containing the message payload.</param>
        public Msg(string subject, string reply, byte[] data)
            : this(subject, reply, null, data)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Msg"/> class with a subject and data.
        /// </summary>
        /// <param name="subject">Subject of the message.</param>
        /// <param name="data">A byte array containing the message payload.</param>
        public Msg(string subject, byte[] data)
            : this(subject, null, data)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Msg"/> class with a subject and no data.
        /// </summary>
        /// <param name="subject">Subject of the message.</param>
        public Msg(string subject)
            : this(subject, null, null)
        {
        }

        internal Msg(MsgArg arg, Subscription s, byte[] payload, long totalLen)
        {
            subject = arg.subject;
            _reply = arg.reply;
            sub = s;

            if (arg.hdr > 0)
            {
                HeaderStatusReader hsr = new HeaderStatusReader(payload, arg.hdr);
                header = hsr.Header;
                status = hsr.Status;
            }

            // make a deep copy of the bytes for this message.
            long payloadLen = totalLen - arg.hdr;
            if (payloadLen > 0)
            {
                data = new byte[payloadLen];
                Array.Copy(payload, arg.hdr, data, 0, (int)(totalLen - arg.hdr));
            }
            else
            {
                data = Empty;
            }
        }

        /// <summary>
        /// Gets or sets the subject.
        /// </summary>
        public string Subject
        {
            get { return subject; }
            set { subject = value; }
        }

        /// <summary>
        /// Gets or sets the reply subject.
        /// </summary>
        public string Reply
        {
            get { return _reply; }
            set { _reply = value; }
        }

        /// <summary>
        /// Gets or sets the payload of the message.
        /// </summary>
        /// <remarks>
        /// This copies application data into the message. See <see cref="AssignData" /> to directly pass the bytes buffer.
        /// </remarks>
        /// <seealso cref="AssignData"/>
        public byte[] Data
        {
            get { return data; }

            set
            {
                if (value == null)
                {
                    this.data = null;
                    return;
                }

                int len = value.Length;
                if (len == 0)
                    this.data = Empty;
                else
                {
                    this.data = new byte[len];
                    Array.Copy(value, 0, data, 0, len);
                }
            }
        }

        /// <summary>
        /// Assigns the data of the message.
        /// </summary>
        /// <remarks>
        /// <para>This is a direct assignment,
        /// to avoid expensive copy operations.  A change to the passed
        /// byte array will be changed in the message.</para>
        /// <para>The calling application is responsible for the data integrity in the message.</para>
        /// </remarks>
        /// <param name="data">a bytes buffer of data.</param>
        public void AssignData(byte[] data)
        {
            this.data = data;
        }

        /// <summary>
        /// Gets the <see cref="ISubscription"/> which received the message.
        /// </summary>
        [ObsoleteAttribute("This property will soon be deprecated. Use ArrivalSubscription instead.")]
        public ISubscription ArrivalSubcription
        {
            get { return sub; }
        }

        /// <summary>
        /// Gets the <see cref="ISubscription"/> which received the message.
        /// </summary>
        public ISubscription ArrivalSubscription
        {
            get { return sub; }
        }

        /// <summary>
        /// Send a response to the message on the arrival subscription.
        /// </summary>
        /// <param name="data">The response payload to send.</param>
        /// <exception cref="NATSException">
        /// <para><see cref="Reply"/> is null or empty.</para>
        /// <para>-or-</para>
        /// <para><see cref="ArrivalSubscription"/> is null.</para>
        /// </exception>
        public void Respond(byte[] data)
        {
            if (String.IsNullOrEmpty(Reply))
            {
                throw new NATSException("No Reply subject");
            }

            Connection conn = ArrivalSubscription?.Connection;
            if (conn == null)
            {
                throw new NATSException("Message is not bound to a subscription");
            }

            conn.Publish(this.Reply, data);
        }

        /// <summary>
        /// Generates a string representation of the messages.
        /// </summary>
        /// <returns>A string representation of the messages.</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{");
            if (header != null)
            {
                sb.AppendFormat("Header={0};", header.ToString());
            }
            sb.AppendFormat("Subject={0};Reply={1};Payload=<", Subject,
                Reply != null ? _reply : "null");

            int len = data.Length;
            int i;

            for (i = 0; i < 32 && i < len; i++)
            {
                sb.Append((char)data[i]);
            }

            if (i < len)
            {
                sb.AppendFormat("{0} more bytes", len - i);
            }

            sb.Append(">}");

            return sb.ToString();
        }

        /// <summary>
        /// Gets or sets the <see cref="MsgHeader"/> of the message.
        /// </summary>
        public MsgHeader Header
        {
            get
            {
                // Auto generate the header when accessed from the
                // caller.
                if (header == null)
                {
                    header = new MsgHeader();
                }
                return header;
            }

            set
            {
                header = value;
            }
        }

        /// <summary>
        /// Returns true if there is a <see cref="MsgHeader"/> with fields set.
        /// </summary>
        public bool HasHeaders => header?.Count > 0;

        /// <summary>
        /// Gets or sets the <see cref="MsgStatus"/> of the message.
        /// </summary>
        public MsgStatus Status => status;

        /// <summary>
        /// Returns true if there is a <see cref="MsgStatus"/> with fields set.
        /// </summary>
        public bool HasStatus
        {
            get
            {
                return status != null;
            }
        }

        #region JetStream

        /// <summary>
        /// Gets the metadata associated with a JetStream message.
        /// </summary>
        public virtual JetStream.MetaData MetaData => null;

        /// <summary>
        /// Acknowledges a JetStream messages received from a Consumer,
        /// indicating the message will not be resent.
        /// </summary>
        /// <remarks>
        /// This is a NOOP for standard NATS messages.
        /// </remarks>
        public virtual void Ack() { /* noop */ }

        /// <summary>
        /// Acknowledges a JetStream message received from a Consumer,
        /// indicating the message should not be received again later.
        /// A timeout of zero does not confirm the acknowledgement.
        /// </summary>
        /// <param name="timeout">the duration to wait for an ack
        /// confirmation</param>
        /// <remarks>
        /// This is a NOOP for standard NATS messages.
        /// </remarks>
        public virtual void AckSync(int timeout) { /* noop */ }

        /// <summary>
        /// Acknowledges a JetStream message has been received but indicates
        /// that the message is not completely processed and should be sent
        /// again later.
        /// </summary>
        /// <remarks>
        /// This is a NOOP for standard NATS messages.
        /// </remarks>
        public virtual void Nak() { /* noop */ }

        /// <summary>
        /// Prevents this message from ever being delivered regardless of
        /// maxDeliverCount.
        /// </summary>
        /// <remarks>
        /// This is a NOOP for standard NATS messages.
        /// </remarks>
        public virtual void Term() {  /* noop */ }

        /// <summary>
        /// Indicates that this message is being worked on and reset redelivery timer in the server.
        /// </summary>
        /// <remarks>
        /// This is a NOOP for standard NATS messages.
        /// </remarks>
        public virtual void InProgress() {  /* noop */ }

        /// <summary>
        /// Checks if a message is from Jetstream or is a standard message.
        /// </summary>
        /// <returns>True if this is a JetStream Message.</returns>
        public virtual bool IsJetStream { get { return false; } }

        #endregion
    }
}
