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
        /// same as null, was used to unset the field in the builder
        /// </summary>
        [Obsolete("Just use null to unset.", false)]
        public const string DefaultStream = null;

        [Obsolete("This property is obsolete, it is not used.", false)]
        public const ulong DefaultLastSequence = 0;

        /// <summary>
        /// the name of the stream to check after the publish has succeeded
        /// </summary>
        [Obsolete("This field isn't really very useful since it's used after the publish.", false)]
        public string Stream { get; }

        /// <summary>
        /// The stream timeout.
        /// </summary>
        public Duration StreamTimeout { get; }
        
        /// <summary>
        /// The Expected Stream.
        /// </summary>
        public string ExpectedStream { get; }
        
        /// <summary>
        /// The Expected Last Message Id.
        /// </summary>
        public string ExpectedLastMsgId { get; }

        [Obsolete("This property is obsolete, it represents a nullable value. ulong.MaxValue is returned for null/no value", false)]
        public ulong ExpectedLastSeq => ExpectedLastSequence.GetValueOrDefault(ulong.MaxValue);
        
        [Obsolete("This property is obsolete, it represents a nullable value. ulong.MaxValue is returned for null/no value", false)]
        public ulong ExpectedLastSubjectSeq => ExpectedLastSubjectSequence.GetValueOrDefault(ulong.MaxValue);
        
        /// <summary>
        /// The Expected Last Sequence. No value if not set
        /// </summary>
        public ulong? ExpectedLastSequence { get; }
        
        /// <summary>
        /// The Expected Last Subject Sequence. No value if not set
        /// </summary>
        public ulong? ExpectedLastSubjectSequence { get; }
        
        /// <summary>
        /// The Expected Subject to limit last subject sequence number of the stream. No value if not set
        /// </summary>
        public string ExpectedLastSubjectSequenceSubject { get; }
        
        /// <summary>
        /// The Expected Message Id.
        /// </summary>
        public string MessageId { get; }

        /// <summary>
        /// Gets the message ttl string. Might be null. Might be "never".
        /// 10 seconds would be "10s" for the server
        /// </summary>
        public string MessageTtl => _messageTtl?.TtlString;
        private MessageTtl _messageTtl;
        
        internal PublishOptions(PublishOptionsBuilder b)
        {
            Stream = b._pubAckStream;
            StreamTimeout = b._streamTimeout;
            ExpectedStream = b._expectedStream;
            ExpectedLastMsgId = b._expectedLastMsgId;
            ExpectedLastSequence = b._expectedLastSeq;
            ExpectedLastSubjectSequence = b._expectedLastSubjectSeq;
            ExpectedLastSubjectSequenceSubject = b._expectedLastSubjectSeqSubject;
            MessageId = b._messageId;
            _messageTtl = b._messageTtl;
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
        /// The PublishOptionsBuilder builds PublishOptions
        /// </summary>
        public sealed class PublishOptionsBuilder
        {
            internal string _pubAckStream = null;
            internal Duration _streamTimeout = DefaultTimeout;
            internal string _expectedStream;
            internal string _expectedLastMsgId;
            internal ulong? _expectedLastSeq;
            internal ulong? _expectedLastSubjectSeq;
            internal string _expectedLastSubjectSeqSubject;
            internal string _messageId;
            internal MessageTtl _messageTtl;
            
            /// <summary>
            /// Set the stream name.
            /// </summary>
            /// <param name="stream">Name of the stream</param>
            /// <returns>The Builder</returns>
            public PublishOptionsBuilder WithStream(string stream)
            {
                _pubAckStream = Validator.ValidateStreamName(stream, false);
                return this;
            }

            /// <summary>
            /// Set the stream timeout with a Duration
            /// </summary>
            /// <param name="timeout">The publish acknowledgement timeout as a Duration.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithTimeout(Duration timeout)
            {
                _streamTimeout = Validator.EnsureNotNullAndNotLessThanMin(timeout, Duration.One, DefaultTimeout);
                return this;
            }

            /// <summary>
            /// Set the stream timeout in milliseconds
            /// </summary>
            /// <param name="timeoutMillis">The publish acknowledgement timeout as millis</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithTimeout(long timeoutMillis)
            {
                _streamTimeout = Validator.EnsureDurationNotLessThanMin(timeoutMillis, Duration.One, DefaultTimeout);
                return this;
            }

            /// <summary>
            /// Set the message id.
            /// </summary>
            /// <param name="msgId">The message ID of these options.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithMessageId(string msgId) 
            {
                _messageId = Validator.ValidateNotEmpty(msgId, nameof(msgId));
                return this;
            }

            /// <summary>
            /// Set the expected stream name.
            /// </summary>
            /// <param name="stream">The expected stream name.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithExpectedStream(string stream)
            {
                _expectedStream = stream;
                return this;
            }

            /// <summary>
            /// Set the expected last message ID.
            /// </summary>
            /// <param name="lastMessageId">The expected last message ID.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithExpectedLastMsgId(string lastMessageId)
            {
                _expectedLastMsgId = Validator.ValidateNotEmpty(lastMessageId, nameof(lastMessageId));
                return this;
            }        

            /// <summary>
            /// Set the expected sequence.
            /// </summary>
            /// <param name="sequence">The expected sequence.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithExpectedLastSequence(ulong sequence)
            {
                if (sequence == ulong.MaxValue)
                {
                    _expectedLastSeq = null;
                }
                else
                {
                    _expectedLastSeq = sequence;
                }
                return this;
            }

            /// <summary>
            /// Set the expected subject sequence.
            /// </summary>
            /// <param name="sequence">The expected subject sequence.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithExpectedLastSubjectSequence(ulong sequence)
            {
                _expectedLastSubjectSeq = sequence;
                return this;
            }

            /// <summary>
            /// Sets the filter subject for the expected last subject sequence
            /// This can be used for a wildcard since it is used
            /// in place of the message subject along with expectedLastSubjectSequence
            /// </summary>
            /// <param name="subject">The filter subject.</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithExpectedLastSubjectSequenceSubject(string subject)
            {
                _expectedLastSubjectSeqSubject = subject;
                return this;
            }

            /// <summary>
            /// Clears the expected so the build can be re-used.
            /// Clears the expectedLastId, expectedLastSequence and messageId fields.
            /// </summary>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder ClearExpected() 
            {
                _expectedLastMsgId = null;
                _expectedLastSeq = null;
                _expectedLastSubjectSeq = null;
                _expectedLastSubjectSeqSubject = null;
                _messageId = null;
                return this;
            }

            /// <summary>
            /// Sets the TTL for this specific message to be published.
            /// Less than 1 has the effect of clearing the message ttl.
            /// </summary>
            /// <param name="msgTtlSeconds">the ttl in seconds</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithMessageTtlSeconds(int msgTtlSeconds)
            {
                _messageTtl = msgTtlSeconds < 1 ? null : Client.JetStream.MessageTtl.Seconds(msgTtlSeconds);
                return this;
            }

            /// <summary>
            /// Sets the TTL for this specific message to be published. Use at your own risk.
            /// The current specification can be found here See <a href="https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-43.md#per-message-ttl">JetStream Per-Message TTL</a>
            /// Null or white space has the effect of clearing the message ttl
            /// </summary>
            /// <param name="msgTtlCustom"> the custom ttl string</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithMessageTtlCustom(string msgTtlCustom)
            {
                _messageTtl = string.IsNullOrWhiteSpace(msgTtlCustom) ? null : Client.JetStream.MessageTtl.Custom(msgTtlCustom);
                return this;
            }

            /// <summary>
            /// Sets the TTL for this specific message to be published and never be expired
            /// </summary>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithMessageTtlNever()
            {
                _messageTtl = Client.JetStream.MessageTtl.Never();
                return this;
            }

            /// <summary>
            /// Sets the TTL for this specific message to be published and never be expired
            /// </summary>
            /// <param name="messageTtl">the message ttl instance</param>
            /// <returns>The PublishOptionsBuilder</returns>
            public PublishOptionsBuilder WithMessageTtl(MessageTtl messageTtl)
            {
                _messageTtl = messageTtl;
                return this;
            }

            /// <summary>
            /// Builds the PublishOptions
            /// </summary>
            /// <returns>The PublishOptions object.</returns>
            public PublishOptions Build() 
            {
                return new PublishOptions(this);
            }
        }
    }
}
