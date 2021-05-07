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

using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public sealed class PublishOptions
    {
        /// <summary>
        /// The default timeout (2000ms)
        /// </summary>
        public static readonly Duration DefaultTimeout = Duration.OfMillis(Defaults.Timeout);

        /// <summary>
        /// The default stream name (unset)
        /// </summary>
        public const string DefaultStream = null;

        /// <summary>
        /// Default Last Sequence Number (unset)
        /// </summary>
        public const long DefaultLastSequence = -1;

        private readonly string _stream = DefaultStream;
        private readonly Duration _streamTimeout = DefaultTimeout;
        private readonly string _expectedStream = null;
        private readonly string _expectedLastMsgId = null;
        private readonly long _expectedLastSeq = DefaultLastSequence;
        private readonly string _messageId = null;

        /// <summary>
        /// Gets the stream name.
        /// </summary>
        public string Stream { get => _stream; }

        /// <summary>
        /// Gets the stream timeout.
        /// </summary>
        public Duration StreamTimeout { get => _streamTimeout; }
        
        /// <summary>
        /// Gets the Expected Stream.
        /// </summary>
        public string ExpectedStream { get => _expectedStream; }
        
        /// <summary>
        /// Gets the Expected Last Message Id.
        /// </summary>
        public string ExpectedLastMsgId { get => _expectedLastMsgId; }
        
        /// <summary>
        /// Gets the Expected Last Sequence.
        /// </summary>
        public long ExpectedLastSeq { get => _expectedLastSeq; }
        
        /// <summary>
        /// Gets the Expected Message Id.
        /// </summary>
        public string MessageId { get => _messageId; }

        private PublishOptions(string stream, Duration streamTimeout, string expectedStream, string expectedLastMsgId, long expectedLastSeq, string messageId)
        {
            _stream = stream;
            _streamTimeout = streamTimeout;
            _expectedStream = expectedStream;
            _expectedLastMsgId = expectedLastMsgId;
            _expectedLastSeq = expectedLastSeq;
            _messageId = messageId;
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
        
        public sealed class PublishOptionsBuilder
        {
            private string _stream = DefaultStream;
            private Duration _streamTimeout = DefaultTimeout;
            private string _expectedStream;
            private string _expectedLastMsgId;
            private long _expectedLastSeq = DefaultLastSequence;
            private string _messageId;
            
            /// <summary>
            /// Set the stream name.
            /// </summary>
            /// <param name="stream">Name of the stream</param>
            /// <returns>The Builder</returns>
            public PublishOptionsBuilder WithStream(string stream) {
                _stream = string.IsNullOrEmpty(stream) ? DefaultStream : Validator.ValidateStreamName(stream);
                return this;
            }

            /// <summary>
            /// Set the stream timeout with a Duration
            /// </summary>
            /// <param name="timeout">The publish acknowledgement timeout.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithTimeout(Duration timeout) {
                _streamTimeout = timeout ?? DefaultTimeout;
                return this;
            }

            /// <summary>
            /// Set the stream timeout in milliseconds
            /// </summary>
            /// <param name="timeout">The publish acknowledgement timeout.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithTimeout(long timeoutMillis) {
                _streamTimeout = timeoutMillis < 1 ? DefaultTimeout : Duration.OfMillis(timeoutMillis);
                return this;
            }

            /// <summary>
            /// Set the message id.
            /// </summary>
            /// <param name="msgID">The message ID of these options.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithMessageId(string msgId) {
                _messageId = Validator.ValidateNotEmpty(msgId, nameof(msgId));
                return this;
            }

            /// <summary>
            /// Set the expected stream name.
            /// </summary>
            /// <param name="stream">The expected stream name.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithExpectedStream(string stream) {
                _expectedStream = stream;
                return this;
            }

            /// <summary>
            /// Set the expected last message ID.
            /// </summary>
            /// <param name="lastMessageID">The expected last message ID.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithExpectedLastMsgId(string lastMessageID) {
                _expectedLastMsgId = Validator.ValidateNotEmpty(lastMessageID, nameof(lastMessageID));
                return this;
            }        

            /// <summary>
            /// Set the expected stream name.
            /// </summary>
            /// <param name="lastSequence">The expected sequence.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithExpectedLastSequence(long lastSequence) {
                _expectedLastSeq = Validator.ValidateNotNegative(lastSequence, nameof(lastSequence));
                return this;
            }

            /// <summary>
            /// Clears the expected so the build can be re-used.
            /// Clears the expectedLastId, expectedLastSequence and messageId fields.
            /// </summary>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder ClearExpected() {
                _expectedLastMsgId = null;
                _expectedLastSeq = DefaultLastSequence;
                _messageId = null;
                return this;
            }

            /// <summary>
            /// Builds the PublishOptions
            /// </summary>
            /// <returns>The PublishOptions object.</returns>
            public PublishOptions Build() {
                return new PublishOptions(_stream, _streamTimeout, _expectedStream, _expectedLastMsgId, _expectedLastSeq, _messageId);
            }
        }
    }
}
