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
using NATS.Client.Internals;

namespace NATS.Client
{
    /// <summary>
    /// The MsgHeader class provides key/value message header support
    /// similar to HTTP headers.
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

        /// <summary>
        /// Status header key.
        /// </summary>
        public const string Status = "Status";

        /// <summary>
        /// Description header key.
        /// </summary>
        public const string Description = "Description";

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
            if (header != null)
            {
                foreach (string s in header.Keys)
                {
                    nvc[s] = header[s];
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
            StringBuilder sb = new StringBuilder(NatsConstants.HeaderVersionBytesPlusCrlf);
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
}
