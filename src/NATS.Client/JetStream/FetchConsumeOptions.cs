// Copyright 2023 The NATS Authors
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

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Consume Options are provided to customize the consume operation. 
    /// </summary>
    public class FetchConsumeOptions : BaseConsumeOptions
    {
        public static readonly FetchConsumeOptions DefaultConsumeOptions = Builder().Build();

        /// <summary>
        /// The maximum number of messages to fetch.
        /// </summary>
        public int MaxMessages => Messages;
        
        /// <summary>
        /// The maximum number of bytes to fetch.
        /// </summary>
        public long MaxBytes => Bytes;

        protected FetchConsumeOptions(IBaseConsumeOptionsBuilder b) : base(b) {}

        /// <summary>
        /// Gets the FetchConsumeOptions builder.
        /// </summary>
        /// <returns>The builder</returns>
        public static FetchConsumeOptionsBuilder Builder()
        {
            return new FetchConsumeOptionsBuilder();
        }

        public sealed class FetchConsumeOptionsBuilder
            : BaseConsumeOptionsBuilder<FetchConsumeOptionsBuilder, FetchConsumeOptions>
        {
            protected override FetchConsumeOptionsBuilder GetThis()
            {
                return this;
            }
            
            /// <summary>
            /// Set the maximum number of messages to fetch and remove any previously set {@link #maxBytes(long)} constraint.
            /// The number of messages fetched will also be constrained by the expiration time.
            /// <para>Less than 1 means default of <see cref="BaseConsumeOptions.DefaultMessageCount"/>.</para>
            /// </summary>
            /// <param name="maxMessages">the number of messages. Must be greater than 0</param>
            /// <returns>the builder</returns>
            public FetchConsumeOptionsBuilder WithMaxMessages(int maxMessages)
            {
                base.WithMessages(maxMessages);
                return base.WithBytes(-1);
            }

            /// <summary>
            /// Set maximum number of bytes to fetch and remove any previously set maxMessages constraint
            /// The number of bytes fetched will also be constrained by the expiration time.
            /// <para>Less than 1 removes any previously set max bytes constraint.</para>
            /// <para>It is important to set the byte size greater than your largest message payload, plus some amount
            /// to account for overhead, otherwise the consume process will stall if there are no messages that fit the criteria.</para>
            /// See <see cref="Msg.ConsumeByteCount"/> 
            /// </summary>
            /// <param name="maxBytes">the maximum bytes</param>
            /// <returns>the builder</returns>
            public FetchConsumeOptionsBuilder WithMaxBytes(int maxBytes) {
                base.WithMessages(-1);
                return base.WithBytes(maxBytes);
            }

            /// <summary>
            /// Set maximum number of bytes or messages to fetch.
            /// The number of messages/bytes fetched will be constrained by whichever constraint, messages or bytes is reached first.
            /// The number of bytes fetched will also be constrained by the expiration time.
            /// <para>Less than 1 max bytes removes any previously set max bytes constraint.</para>
            /// <para>Less than 1 max messages removes any previously set max messages constraint.</para>
            /// <para>It is important to set the byte size greater than your largest message payload, plus some amount
            /// to account for overhead, otherwise the consume process will stall if there are no messages that fit the criteria.</para>
            /// See <see cref="Msg.ConsumeByteCount"/> 
            /// </summary>
            /// <param name="maxBytes">the maximum bytes</param>
            /// <param name="maxMessages">the maximum number of messages</param>
            /// <returns>the builder</returns>
            public FetchConsumeOptionsBuilder WithMax(int maxBytes, int maxMessages) {
                base.WithMessages(maxMessages);
                return base.WithBytes(maxBytes);
            }

            /// <summary>
            /// Set no wait to true.
            /// When no wait is true, the fetch will return immediately with as many messages as are available. Between zero and the maximum configured.
            /// </summary>
            /// <returns>the builder</returns>
            public FetchConsumeOptionsBuilder WithNoWait()
            {
                _noWait = true;
                _expiresIn = ConsumerConfiguration.IntUnset;
                return this;
            }
            
            /// <summary>
            /// Set no wait to true with an expiration. This is the common configuration to receive messages as soon as they arrive in the stream without excessive pulling.
            /// When no wait is true with expire, the fetch will return immediately with as many messages as are available, but at least one message. Between one and the maximum configured.
            /// When no message is available it will wait for new messages to arrive till it expires.
            /// </summary>
            /// <param name="expiresInMillis"></param>
            /// <returns>the builder</returns>
            public FetchConsumeOptionsBuilder WithNoWaitExpiresIn(int expiresInMillis)
            {
                _noWait = true;
                if (expiresInMillis < 1) {
                    _expiresIn = ConsumerConfiguration.IntUnset;
                    return this;
                }
                return WithExpiresIn(expiresInMillis);
            }

            /// <summary>
            /// Build the FetchConsumeOptions
            /// </summary>
            /// <returns>a FetchConsumeOptions instance</returns>
            public override FetchConsumeOptions Build()
            {
                return new FetchConsumeOptions(this);
            }
        }
    }
}
