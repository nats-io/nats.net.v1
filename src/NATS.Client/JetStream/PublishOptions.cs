// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Text.RegularExpressions;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// The PublishOptions class specifies the options for publishing with JetStream enabled servers.
    /// Options are created using the <see cref="PublishOptions.Builder"/>.
    /// </summary>
    public class PublishOptions
    {
        private readonly string stream = DefaultStream;
        private readonly long streamTimeout = DefaultTimeout;
        private readonly string expectedStream = null;
        private readonly string expectedLastId = null;
        private readonly long expectedLastSeq = DefaultLastSequence;
        private readonly string msgId = null;

        /// <summary>
        /// The default timeout (2000ms)
        /// </summary>
        public static readonly long DefaultTimeout = Defaults.Timeout;

        /// <summary>
        /// The default stream name (unset)
        /// </summary>
        public static readonly string DefaultStream = null;

        /// <summary>
        /// Default Last Sequence Number (unset)
        /// </summary>
        public static readonly long DefaultLastSequence = -1;

        /// <summary>
        /// Gets the stream name.
        /// </summary>
        public string Stream { get => stream; }

        /// <summary>
        /// Gets the stream timeout.
        /// </summary>
        public long StreamTimeout { get => streamTimeout; }

        /// <summary>
        /// Gets the Expected Stream.
        /// </summary>
        public string ExpectedStream { get => expectedStream; }

        /// <summary>
        /// Gets the Expected Stream.
        /// </summary>
        public string ExpectedLastId { get => expectedLastId; }

        /// <summary>
        /// Gets the Expected Stream.
        /// </summary>
        public long ExpectedLastSeq { get => expectedLastSeq; }

        /// <summary>
        /// Gets the Message ID.
        /// </summary>
        public string MsgId { get => msgId; }

        private PublishOptions(
            string stream, long streamTimeout, string expectedStream,
            string expectedLastId, long expectedLastSeq, string msgId)
        {
            this.stream = stream;
            this.streamTimeout = streamTimeout;
            this.expectedStream = expectedStream;
            this.expectedLastId = expectedLastId;
            this.expectedLastSeq = expectedLastSeq;
            this.msgId = msgId;
        }

        /// <summary>
        /// Gets the publish options builder.
        /// </summary>
        /// <returns>
        /// The builder
        /// </returns>
        public static PublishOptionsBuilder Builder()
        {
            return new PublishOptionsBuilder();
        }

        /// <summary>
        /// Builds a PublishOptions object.
        /// </summary>
        public class PublishOptionsBuilder
        {
            string stream = DefaultStream;
            long streamTimeout = DefaultTimeout;
            string expectedStream = null;
            string expectedLastId = null;
            long expectedLastSeq = DefaultLastSequence;
            string msgId = null;

            /// <summary>
            /// Builds the PublishOptions
            /// </summary>
            /// <returns>
            /// The PublishOptions object.
            /// </returns>
            public PublishOptions Build()
            {
                return new PublishOptions(stream, streamTimeout,
                    expectedStream, expectedLastId, expectedLastSeq,
                    msgId);
            }

            private static readonly Regex regex = new Regex("^[a-zA-Z0-9-]*$");

            private string checkStreamName(string stream)
            {
                if (stream == null)
                    return null;

                if (stream.Length == 0)
                    return null;

                if (regex.IsMatch(stream))
                    return stream;

                throw new ArgumentException("stream cannot contain *, >, whitespace, or .");
            }

            /// <summary>
            /// Set the stream name.
            /// </summary>
            /// <param name="stream">Name of the stream</param>
            /// <returns></returns>
            public PublishOptionsBuilder WithStream(string stream)
            {
                this.stream = checkStreamName(stream);
                return this;
            }

            /// <summary>
            /// Set the stream timeout.
            /// </summary>
            /// <param name="timeout">The publish acknowledgement timeout.</param>
            /// <returns></returns>
            public PublishOptionsBuilder WithTimeout(int timeout)
            {
                this.streamTimeout = timeout;
                return this;
            }

            /// <summary>
            /// Set the message ID.
            /// </summary>
            /// <param name="msgID">The message ID of these options.</param>
            /// <returns></returns>
            public PublishOptionsBuilder WithMsgId(string msgID)
            {
                if (string.IsNullOrEmpty(msgID))
                    throw new ArgumentException("Cannot be null or empty", nameof(msgID));

                this.msgId = msgID;
                return this;
            }

            /// <summary>
            /// Set the expected stream name.
            /// </summary>
            /// <param name="stream">The expected stream name.</param>
            /// <returns></returns>
            public PublishOptionsBuilder WithExpectedStream(string stream)
            {
                this.expectedStream = checkStreamName(stream);
                return this;
            }

            /// <summary>
            /// Set the expected stream name.
            /// </summary>
            /// <param name="msgID">The expected previous message ID.</param>
            /// <returns></returns>
            public PublishOptionsBuilder WithLastExpectedMsgID(string msgID)
            {
                if (string.IsNullOrEmpty(msgID))
                    throw new ArgumentException("cannot be null or empty", nameof(msgID));

                this.expectedLastId = msgID;
                return this;
            }

            /// <summary>
            /// Set the expected stream name.
            /// </summary>
            /// <param name="msgID">The expected previous message ID.</param>
            /// <returns></returns>
            public PublishOptionsBuilder WithLastExpectedSequence(long sequence)
            {
                if (sequence < 0)
                {
                    throw new ArgumentException("cannot be negative", nameof(sequence));
                }
                this.expectedLastSeq = sequence;
                return this;
            }
        }
    }

}
