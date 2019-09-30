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
using System.Text;

namespace NATS.Client
{
    /// <summary>
    /// A NATS message is an object encapsulating a subject, optional reply
    /// payload, and subscription information, sent or received by the client
    /// application.
    /// </summary>
    public sealed class Msg
    {
        private static readonly byte[] Empty = new byte[0];
        private string subject;
        private string reply;
        private byte[] data;
        internal Subscription sub;

        /// <summary>
        /// Initializes a new instance of the <see cref="Msg"/> class without any
        /// subject, reply, or data.
        /// </summary>
        public Msg()
        {
            subject = null;
            reply   = null;
            data    = null;
            sub     = null;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Msg"/> class with a subject, reply, and data.
        /// </summary>
        /// <param name="subject">Subject of the message.</param>
        /// <param name="reply">A reply subject, or <c>null</c>.</param>
        /// <param name="data">A byte array containing the message payload.</param>
        public Msg(string subject, string reply, byte[] data)
        {
            if (string.IsNullOrWhiteSpace(subject))
            {
                throw new ArgumentException(
                    "Subject cannot be null, empty, or whitespace.",
                    "subject");
            }

            this.Subject = subject;
            this.Reply = reply;
            this.Data = data;
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

        internal Msg(MsgArg arg, Subscription s, byte[] payload, long length)
        {
            subject = arg.subject;
            reply   = arg.reply;
            sub     = s;

            // make a deep copy of the bytes for this message.
            if (length > 0)
            {
                data = new byte[length];
                Array.Copy(payload, 0, data, 0, (int)length);
            }
            else
                data = Empty;
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
            get { return reply; }
            set { reply = value; }
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
        public ISubscription ArrivalSubcription
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
        /// <para><see cref="ArrivalSubcription"/> is null.</para>
        /// </exception>
        public void Respond(byte[] data)
        {
            if (String.IsNullOrEmpty(Reply))
            {
                throw new NATSException("No Reply subject");
            }

            Connection conn = ArrivalSubcription?.Connection;
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
            sb.AppendFormat("Subject={0};Reply={1};Payload=<", Subject,
                Reply != null ? reply : "null");

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
    }


}