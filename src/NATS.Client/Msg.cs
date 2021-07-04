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

namespace NATS.Client
{
    /// <summary>
    /// A NATS message is an object encapsulating a subject, optional reply
    /// payload, optional header, and subscription information, sent or
    /// received by the client application.
    /// </summary>
    public class Msg
    {
        private static readonly byte[] Empty = new byte[0];
        private string subject;
        private string reply;
        private byte[] data;
        internal Subscription sub;
        internal MsgHeader header;

        /// <summary>
        /// Initializes a new instance of the <see cref="Msg"/> class without any
        /// subject, reply, or data.
        /// </summary>
        public Msg()
        {
            subject = null;
            reply = null;
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
            reply = arg.reply;
            sub = s;

            if (arg.hdr > 0)
            {
                header = ReadHeaderStatusBytes(payload, arg.hdr);
            }

            // make a deep copy of the bytes for this message.
            long payloadLen = totalLen - arg.hdr;
            if (payloadLen > 0)
            {
                data = new byte[payloadLen];
                Array.Copy(payload, (int)arg.hdr, data, 0, (int)(totalLen - arg.hdr));
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
        public bool HasHeaders
        {
            get
            {
                return header?.Count > 0;
            }
        }

        #region JetStream

        /// <summary>
        /// Gets the metadata associated with a JetStream message.
        /// </summary>
        public virtual JetStream.MetaData MetaData { get { return null; } }

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

        /// <summary>
        /// Initializes a new instance of the MsgHeader class.
        /// </summary>
        /// <param name="bytes">A byte array of a serialized MsgHeader class.</param>
        /// <param name="byteCount">Count of bytes in the serialized array.</param>
        internal static MsgHeader ReadHeaderStatusBytes(byte[] bytes, int byteCount) {
            if (byteCount < 1)
            {
                throw new NATSException("invalid byte count");
            }
            if (bytes == null)
            {
                throw new NATSException("invalid byte array");
            }
            if (bytes.Length < byteCount)
            {
                throw new NATSException("count exceeds byte array length");
            }
            if (byteCount < MsgHeader.MinimalValidHeaderLen)
            {
                throw new NATSInvalidHeaderException();
            }

            // check for the trailing \r\n\r\n
            if (bytes[byteCount-4] != '\r' || bytes[byteCount - 3] != '\n' ||
                bytes[byteCount-2] != '\r' || bytes[byteCount - 1] != '\n')
            {
                ThrowInvalidHeaderException(bytes, byteCount);
            }

            MsgHeader msgHeader = new MsgHeader();
            
            // normally, start KV pairs after the standard header.
            // this may change with inlined status.
            int kvStart = MsgHeader.HeaderLen;

            // We need to be fast so do a byte comparison.
            int i;
            for (i = 0; i < MsgHeader.HeaderLen; i++)
            {
                if (bytes[i] != MsgHeader.HeaderBytes[i])
                {
                    // check for inlined status:  e.g. "NATS/1.0 503\r\n\r\n"
                    if (bytes[i] == ' ')
                    {
                        try
                        {
                            // could assume three digits, but want to be a bit more
                            // future proof in case we someday insert something
                            // else there.  So find the \r\n
                            int start = i + 1;
                            int end; 
                            for (end = start;
                                end < byteCount - 1 && bytes[end] != '\r' && bytes[end + 1] != '\n';
                                    end++);

                            string status = Encoding.UTF8.GetString(bytes, start, end - start);

                            // set the start for kv pairs at end of the status + \r\n.
                            kvStart = end + 2;

                            // Fastpath - we only have a code...
                            if (status.Length == 3)
                            {
                                msgHeader.Add(MsgHeader.Status, status);
                                break;
                            }

                            // We can have code and description.

                            // trim any leading whitespace.
                            status = status.TrimStart();

                            // check for description.
                            int spaceIdx = status.IndexOf(' ');
                            if (spaceIdx < 0)
                            {
                                // there was whitespace and no description.
                                msgHeader.Add(MsgHeader.Status, status);
                            }
                            else
                            {
                                // There is a code and description...
                                msgHeader.Add(MsgHeader.Status, status.Substring(0, spaceIdx));
                                msgHeader.Add(MsgHeader.Description, status.Substring(spaceIdx + 1));
                            }

                            // we're done
                            break;
                        }
                        catch
                        {
                            ThrowInvalidHeaderException(bytes, byteCount);
                        }
                    }
                    else
                    {
                        ThrowInvalidHeaderException(bytes, byteCount);

                    }
                }
            }

            int loc = 0;
            string key = null;
            char[] stringBuf = new char[64];

            // must start on a key
            for (i = kvStart; i < byteCount - 2; i++)
            {
                if (bytes[i] == ':' && key == null)
                {
                    if (loc == 0)
                    {
                        // empty key
                        throw new NATSInvalidHeaderException("Missing key");
                    }
                    key = new string(stringBuf, 0, loc).TrimEnd();
                    loc = 0;
                }
                else if (bytes[i] == '\r' && bytes[i + 1] == '\n')
                {
                    if (key == null)
                    {
                        throw new NATSInvalidHeaderException("Missing key.");
                    }
                    // empty value
                    if (loc == 0)
                    {
                        // empty value
                        msgHeader.Add(key, "");
                    }
                    else
                    {
                        msgHeader.Add(key, new string(stringBuf, 0, loc));
                    }

                    // reset the key, buffer location, and skip past the \n.
                    key = null;
                    loc = 0;
                    i++;
                }
                else
                {
                    // it's a key or value

                    // check our header length and resize if need be.
                    if (loc > stringBuf.Length - 1)
                    {
                        // For simplicity, just resize it to the entire header kv size.
                        char[] buf = new char[byteCount - MsgHeader.HeaderLen];
                        Array.Copy(stringBuf, buf, stringBuf.Length);
                        stringBuf = buf;
                    }

                    // copy the key or value byte into our buffer.
                    stringBuf[loc] = (char)bytes[i];
                    loc++;
                }
            }

            return msgHeader;
        }

        private static void ThrowInvalidHeaderException(byte[] bytes, int byteCount)
        {
            throw new NATSInvalidHeaderException("Invalid header: " +
                                                 Encoding.UTF8.GetString(bytes, 0, byteCount));
        }
    }
}
