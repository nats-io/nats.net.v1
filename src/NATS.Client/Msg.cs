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
using System.Collections;
using System.Collections.Specialized;
using System.Text;

namespace NATS.Client
{
    /// <summary>
    /// The MsgHeader class provides key/value message header support
    /// simlilar to HTTP headers.
    /// </summary>
    /// <remarks>
    /// Keys and values may only contain printable ASCII character values and
    /// cannot contain `:`.  Concurrent access may result in undefined
    /// behavior.
    /// </remarks>
    /// <example>
    /// Setting a header field in a message:
    /// <code>
    /// var m = new Msg();
    /// m.Header["Content-Type"] = "json";
    /// </code>
    /// 
    /// Getting a header field from a message:
    /// <code>
    /// string contentType = m.Header["Content-Type"];
    /// </code>
    ///
    /// To set multiple values:
    /// <code>
    /// m.Header.Add("foo", "value1");
    /// m.Header.Add("foo", "value2");
    /// </code>
    /// Get multiple values:
    /// <code>
    /// string []values = m.Header.GetValues("foo");
    /// </code>
    /// </example>
    public sealed class MsgHeader : IEnumerable
    {
        // Message headers are in the form of:
        // |HEADER|crlf|key1:value1|crlf|key2:value2|crlf|...|crlf
        // e.g. MATS/1.0\r\nkey1:value1\r\nkey2:value2\r\n\r\n

        // Define message header version string and size.
        internal static readonly string Header = "NATS/1.0\r\n";
        internal static readonly byte[] HeaderBytes = Encoding.UTF8.GetBytes(Header);
        internal static readonly int HeaderLen = HeaderBytes.Length;
        internal static readonly int MinimalValidHeaderLen = Encoding.UTF8.GetBytes(Header + "\r\n").Length;


        /// <summary>
        /// Status header key.
        /// </summary>
        public static readonly string Status = "Status";

        /// <summary>
        /// Description header key.
        /// </summary>
        public static readonly string Description = "Description";

        /// <summary>
        /// No Responders Status code, 503.
        /// </summary>
        public static readonly string NoResponders = "503";

        /// <summary>
        /// Not Found Status code, 404.
        /// </summary>
        public static readonly string NotFound = "404";

        // Cache the serialized headers to optimize reuse
        private byte[] bytes = null;

        private readonly NameValueCollection nvc = new NameValueCollection();

        /// <summary>
        /// Initializes a new empty instance of the MsgHeader class.
        /// </summary>
        public MsgHeader() : base() { }

        /// <summary>
        /// Copies the entries from an existing MsgHeader instance to a
        /// new MsgHeader instance.
        /// </summary>
        /// <remarks>
        /// The header cannot be empty or contain invalid fields.
        /// </remarks>
        /// <param name="header">the NATS message header to copy.</param>
        public MsgHeader(MsgHeader header)
        {
            if (header == null)
            {
                throw new ArgumentNullException("header");
            }
            if (header.Count == 0)
            {
                throw new ArgumentException("header", "header cannot be empty");
            }
            foreach (string s in header.Keys)
            {
                nvc[s] = header[s];
            }
        }

        // buffer for parsing strings
        private char[] StringBuf = new char[64];

        private static void ThrowInvalidHeaderException(byte[] bytes, int byteCount)
        {
            throw new NATSInvalidHeaderException("Invalid header: " +
                  Encoding.UTF8.GetString(bytes, 0, byteCount));
        }

        /// <summary>
        /// Initializes a new instance of the MsgHeader class.
        /// </summary>
        /// <param name="bytes">A byte array of a serialized MsgHeader class.</param>
        /// <param name="byteCount">Count of bytes in the serialized array.</param>
        internal MsgHeader(byte[] bytes, int byteCount) : base()
        {
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
                throw new NATSException("count exceeeds byte array length");
            }
            if (byteCount < MinimalValidHeaderLen)
            {
                throw new NATSInvalidHeaderException();
            }

            // check for the trailing \r\n\r\n
            if (bytes[byteCount-4] != '\r' || bytes[byteCount - 3] != '\n' ||
                bytes[byteCount-2] != '\r' || bytes[byteCount - 1] != '\n')
            {
                ThrowInvalidHeaderException(bytes, byteCount);
            }

            // normally, start KV pairs after the standard header.
            // this may change with inlined status.
            int kvStart = HeaderLen;

            // We need to be fast so do a byte comparison.
            int i;
            for (i = 0; i < HeaderLen; i++)
            {
                if (bytes[i] != HeaderBytes[i])
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
                                Add(Status, status);
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
                                Add(Status, status);
                            }
                            else
                            {
                                // There is a code and description...
                                Add(Status, status.Substring(0, spaceIdx));
                                Add(Description, status.Substring(spaceIdx + 1));
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
                    key = new string(StringBuf, 0, loc).TrimEnd();
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
                        Add(key, "");
                    }
                    else
                    {
                        Add(key, new string(StringBuf, 0, loc));
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
                    if (loc > StringBuf.Length - 1)
                    {
                        // For simplicity, just resize it to the entire header kv size.
                        char[] buf = new char[byteCount - HeaderLen];
                        Array.Copy(StringBuf, buf, StringBuf.Length);
                        StringBuf = buf;
                    }

                    // copy the key or value byte into our buffer.
                    StringBuf[loc] = (char)bytes[i];
                    loc++;
                }
            }
        }

        private void CheckKeyValue(string key, string value)
        {
            // values are OK to be null, check keys elsewhere.
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentException("key cannot be empty or null.");
            }

            foreach (char c in key)
            {
                // only printable characters and no colon
                if (c < 32 || c > 126 || c == ':')
                    throw new ArgumentException(string.Format("Invalid character {0:X2} in key.", c));
            }

            if (value != null)
            {
                foreach (char c in value)
                {
                    // Generally more permissive than HTTP.  Allow only printable
                    // characters and include tab (0x9) to cover what's allowed
                    // in quoted strings and comments.
                    if ((c < 32 && c != 9) || c > 126)
                        throw new ArgumentException(string.Format("Invalid character {0:X2} in value.", c));
                }
            }
        }

        /// <summary>
        /// Gets an enumerator for the keys.
        /// </summary>
        public IEnumerable Keys
        {
            get {
                return nvc.Keys;
            }
        }

        /// <summary>
        /// Gets the current number of header entries.
        /// </summary>
        public int Count
        {
            get { return nvc.Count;  }
        }

        /// <summary>
        /// Gets or sets the string entry with the specified string key in the message header.
        /// </summary>
        /// <param name="name">The string key of the entry to locate. The key cannot be null, empty, or whitespace.</param>
        /// <returns>A string that contains the comma-separated list of values associated with the specified key, if found; otherwise, null</returns>
        public string this[string name]
        {
            get
            {
                return nvc[name];
            }

            set
            {
                CheckKeyValue(name, value);

                nvc[name] = value;

                // Trigger serialization the next time ToByteArray is called.
                bytes = null;
            }
        }

        /// <summary>
        /// Add a header field with the specified name and value.
        /// </summary>
        /// <param name="name">Name of the header field.</param>
        /// <param name="value">Value of the header field.</param>
        public void Add(string name, string value)
        {
            CheckKeyValue(name, value);
            nvc.Add(name, value);
            bytes = null;
        }

        /// <summary>
        /// Sets the value of a message header field.
        /// </summary>
        /// <param name="name">Name of the header field to set.</param>
        /// <param name="value">Value of the header field.</param>
        public void Set(string name, string value)
        {
            CheckKeyValue(name, value);
            nvc.Set(name, value);
            bytes = null;
        }

        /// <summary>
        /// Remove a header entry.
        /// </summary>
        /// <param name="name">Name of the header field to remove.</param>
        public void Remove(string name)
        {
            nvc.Remove(name);
        }

        /// <summary>
        /// Removes all entries from the message header.
        /// </summary>
        public void Clear()
        {
            nvc.Clear();
            bytes = null;
        }

        private string ToHeaderString()
        {
            StringBuilder sb = new StringBuilder(MsgHeader.Header);
            foreach (string s in nvc.Keys)
            {
                foreach (string v in nvc.GetValues(s))
                {
                    sb.AppendFormat("{0}:{1}\r\n", s, v);
                }
            }
            sb.Append("\r\n");
            return sb.ToString();
        }

        internal byte[] ToByteArray()
        {
            // An empty set of headers should be treated as a message with no
            // header.
            if (nvc.Count == 0)
            {
                return null;
            }

            if (bytes == null)
            {
                bytes = Encoding.UTF8.GetBytes(ToHeaderString());
            }
            return bytes;
        }

        /// <summary>
        /// Gets all values of a header field.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public string[] GetValues(string name)
        {
            return nvc.GetValues(name);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the message header
        /// keys.
        /// </summary>
        /// <returns></returns>
        public IEnumerator GetEnumerator()
        {
            return nvc.GetEnumerator();
        }
    }

    /// <summary>
    /// A NATS message is an object encapsulating a subject, optional reply
    /// payload, optional header, and subscription information, sent or
    /// received by the client application.
    /// </summary>
    public sealed class Msg
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
                header = new MsgHeader(payload, arg.hdr);
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
    }
}
