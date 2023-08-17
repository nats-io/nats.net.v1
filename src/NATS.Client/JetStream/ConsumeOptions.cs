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
    public class ConsumeOptions : BaseConsumeOptions
    {
        public static readonly ConsumeOptions DefaultConsumeOptions = Builder().Build();

        /// <summary>
        /// The initial batch size in messages.
        /// </summary>
        public int BatchSize => Messages;
        
        /// <summary>
        /// The initial batch size in bytes.
        /// </summary>
        public long BatchBytes => Bytes;

        protected ConsumeOptions(IBaseConsumeOptionsBuilder b) : base(b) {}

        /// <summary>
        /// Gets the ConsumeOptions builder.
        /// </summary>
        /// <returns>The builder</returns>
        public static ConsumeOptionsBuilder Builder()
        {
            return new ConsumeOptionsBuilder();
        }

        public sealed class ConsumeOptionsBuilder
            : BaseConsumeOptionsBuilder<ConsumeOptionsBuilder, ConsumeOptions>
        {
            protected override ConsumeOptionsBuilder GetThis()
            {
                return this;
            }
            
            /// <summary>
            /// Set the initial batch size in messages and remove any previously set batch byte constraint.
            /// <para>Less than 1 will assume the default of <inheritdoc cref="BaseConsumeOptions.DefaultMessageCount"/> when bytes are not specified.
            /// When bytes are specified, the batch messages size is set to prioritize the batch byte amount.</para>
            /// </summary>
            /// <param name="batchSize">the batch size in messages</param>
            /// <returns>the builder</returns>
            public ConsumeOptionsBuilder WithBatchSize(int batchSize)
            {
                base.WithMessages(batchSize);
                return base.WithBytes(-1);
            }

            /// <summary>
            /// Set the initial batch size in bytes and remove any previously set batch message constraint.
            /// Less than 1 removes any previously set batch byte constraint.
            /// <para>When setting bytes to non-zero, the batch messages size is set to prioritize the batch byte size.</para>
            /// <para>Also, it is important to set the byte size greater than your largest message payload, plus some amount
            /// to account for overhead, otherwise the consume process will stall if there are no messages that fit the criteria.</para>
            /// See <see cref="Msg.ConsumeByteCount"/>
            /// </summary>
            /// <param name="batchBytes">the batch size in bytes</param>
            /// <returns>the builder</returns>
            public ConsumeOptionsBuilder WithBatchBytes(long batchBytes) {
                base.WithMessages(-1);
                return base.WithBytes(batchBytes);
            }

            /// <summary>
            /// Build the ConsumeOptions
            /// </summary>
            /// <returns>a ConsumeOptions instance</returns>
            public override ConsumeOptions Build()
            {
                return new ConsumeOptions(this);
            }
        }
    }
}
