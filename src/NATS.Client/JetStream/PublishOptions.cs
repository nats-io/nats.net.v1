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
       
        // Use this variable for timeout in publish options.
        internal static readonly Duration DefaultTimeout = Duration.OfMillis(Defaults.Timeout);

        // Use this variable to unset a stream in publish options.
        internal const string UnsetStream = null;

        // Use this variable to unset a sequence number in publish options.
        internal const long UnsetLastSequence = -1;

        public string Stream { get; }
        public Duration StreamTimeout { get; }
        public string ExpectedStream { get; }
        public string ExpectedLastMsgId { get; }
        public long ExpectedLastSeq { get; }
        public string MessageId { get; }

        public PublishOptions(string stream, Duration streamTimeout, string expectedStream, string expectedLastId, long expectedLastSeq, string msgId)
        {
            Stream = stream;
            StreamTimeout = streamTimeout;
            ExpectedStream = expectedStream;
            ExpectedLastMsgId = expectedLastId;
            ExpectedLastSeq = expectedLastSeq;
            MessageId = msgId;
        }

        public sealed class Builder
        {
            private string _stream = UnsetStream;
            private Duration _streamTimeout = DefaultTimeout;
            private string _expectedStream;
            private string _expectedLastId;
            private long _expectedLastSeq = UnsetLastSequence;
            private string _msgId;
            
            /**
             * Sets the stream name for publishing.  The default is undefined.
             * @param stream The name of the stream.
             * @return Builder
             */
            public Builder Stream(string stream) {
                _stream = string.IsNullOrEmpty(stream) ? UnsetStream : Validator.ValidateStreamName(stream);
                return this;
            }

            /**
             * Sets the timeout to wait for a publish acknowledgement from a JetStream
             * enabled NATS server.
             * @param timeout the publish timeout.
             * @return Builder
             */
            public Builder StreamTimeout(Duration timeout) {
                _streamTimeout = timeout ?? DefaultTimeout;
                return this;
            }

            /**
             * Sets the expected stream of the publish. If the 
             * stream does not match the server will not save the message.
             * @param stream expected stream
             * @return builder
             */
            public Builder ExpectedStream(string stream) {
                _expectedStream = stream;
                return this;
            }

            /**
             * Sets the expected last ID of the previously published message.  If the 
             * message ID does not match the server will not save the message.
             * @param lastMsgId the stream
             * @return builder
             */
            public Builder ExpectedLastMsgId(string lastMsgId) {
                _expectedLastId = lastMsgId;
                return this;
            }        

            /**
             * Sets the expected message ID of the publish
             * @param sequence the expected last sequence number
             * @return builder
             */
            public Builder ExpectedLastSequence(long sequence) {
                _expectedLastSeq = sequence;
                return this;
            }

            /**
             * Sets the message id. Message IDs are used for de-duplication
             * and should be unique to each message payload.
             * @param msgId the unique message id.
             * @return publish options
             */
            public Builder MessageId(string msgId) {
                _msgId = msgId;
                return this;
            }

            /**
             * Clears the expected so the build can be re-used.
             * Clears the expectedLastId, expectedLastSequence and messageId fields.
             * @return publish options
             */
            public Builder ClearExpected() {
                _expectedLastId = null;
                _expectedLastSeq = UnsetLastSequence;
                _msgId = null;
                return this;
            }

            /**
             * Builds the publish options.
             * @return publish options
             */
            public PublishOptions Build() {
                return new PublishOptions(_stream, _streamTimeout, _expectedStream, _expectedLastId, _expectedLastSeq, _msgId);
            }
        }
    }
}
