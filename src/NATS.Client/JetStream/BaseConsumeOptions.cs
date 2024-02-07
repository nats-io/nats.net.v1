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

using System;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Base Consume Options are provided to customize the way the consume and 
    /// fetch operate. It is the base class for ConsumeOptions and FetchConsumeOptions.
    /// </summary>
    public class BaseConsumeOptions
    {
        /// <summary>500</summary>
        public const int DefaultMessageCount = 500;
        
        /// <summary>1_000_000</summary>
        public const int DefaultMessageCountWhenBytes = 1_000_000;
        
        /// <summary>25</summary>
        public const int DefaultThresholdPercent = 25;
        
        /// <summary>30_000</summary>
        public const int DefaultExpiresInMillis = 30000;
        
        /// <summary>100</summary>
        public const int MinExpiresMills = 100;
        
        /// <summary>30_000</summary>
        public const int MaxHearbeatMillis = 30000;
        
        /// <summary>50</summary>
        public const int MaxIdleHeartbeatPercent = 50;

        public int Messages { get; }
        public long Bytes { get; }
        public int ExpiresInMillis { get; }
        public int IdleHeartbeat { get; }
        public int ThresholdPercent { get; }

        public override string ToString()
        {
            return $"Messages: {Messages}, Bytes: {Bytes}, ExpiresIn: {ExpiresInMillis}, IdleHeartbeat: {IdleHeartbeat}, ThresholdPercent: {ThresholdPercent}";
        }

        protected BaseConsumeOptions(IBaseConsumeOptionsBuilder b)
        {
            Bytes = b.Bytes;
            if (Bytes > 0)
            {
                Messages = b.Messages == -1 ? DefaultMessageCountWhenBytes : b.Messages;
            }
            else {
                Messages = b.Messages == -1 ? DefaultMessageCount : b.Messages;
            }

            // validation handled in builder
            ThresholdPercent = b.ThresholdPercent;
            ExpiresInMillis = b.ExpiresIn;

            // calculated
            IdleHeartbeat = Math.Min(MaxHearbeatMillis, ExpiresInMillis * MaxIdleHeartbeatPercent / 100);
        }
        
        public interface IBaseConsumeOptionsBuilder
        {
            int Messages { get; }
            long Bytes { get; }
            int ThresholdPercent { get; }
            int ExpiresIn { get; }
        }

        public abstract class BaseConsumeOptionsBuilder<TB, TCo> : IBaseConsumeOptionsBuilder
        {
            int _messages = -1;
            long _bytes = 0;
            int _thresholdPercent = DefaultThresholdPercent;
            int _expiresIn = DefaultExpiresInMillis;

            public int Messages => _messages;
            public long Bytes => _bytes;
            public int ThresholdPercent => _thresholdPercent;
            public int ExpiresIn => _expiresIn;

            protected abstract TB GetThis();
            
            protected TB WithMessages(int messages) {
                this._messages = messages < 1 ? -1 : messages;
                return GetThis();
            }

            protected TB WithBytes(long bytes) {
                _bytes = bytes < 1 ? 0 : bytes;
                return GetThis();
            }

            /// <summary>
            /// In Fetch, sets the maximum amount of time to wait to reach the batch size or max byte.
            /// In Consume, sets the maximum amount of time for an individual pull to be open
            /// before issuing a replacement pull.
            /// Zero or less will default to  <inheritdoc cref="BaseConsumeOptions.DefaultExpiresInMillis"/>,
            /// otherwise, cannot be less than <inheritdoc cref="MinExpiresMills"/>.
            /// </summary>
            /// <param name="expiresInMillis">the expiration time in milliseconds</param>
            /// <returns>the builder</returns>
            public TB WithExpiresIn(int expiresInMillis) {
                this._expiresIn = expiresInMillis;
                if (expiresInMillis < 1) {
                    _expiresIn = DefaultExpiresInMillis;
                }
                else if (expiresInMillis < MinExpiresMills) {
                    throw new ArgumentException($"Expires must be greater than or equal to {MinExpiresMills}");
                }
                else {
                    _expiresIn = expiresInMillis;
                }

                return GetThis();
            }

            /// <summary>
            /// Set the threshold percent of max bytes (if max bytes is specified) or messages
            /// that will trigger issuing pull requests to keep messages flowing.
            /// <para>Only applies to endless consumes</para>
            /// <para>For instance if the batch size is 100 and the re-pull percent is 25,
            /// the first pull will be for 100, and then when 25 messages have been received
            /// another 75 will be requested, keeping the number of messages in transit always at 100.</para>
            /// <para>Must be between 1 and 100 inclusive.
            /// Less than 1 will assume the default of <inheritdoc cref="DefaultThresholdPercent"/>.
            /// Greater than 100 will assume 100.</para>
            /// </summary>
            /// <param name="thresholdPercent">the threshold percent</param>
            /// <returns>the builder</returns>
            public TB WithThresholdPercent(int thresholdPercent) {
                this._thresholdPercent = thresholdPercent < 1 ? DefaultThresholdPercent : Math.Min(100, thresholdPercent);
                return GetThis();
            }

            /// <summary>
            /// Build the options.
            /// </summary>
            /// <returns>the built options</returns>
            public abstract TCo Build();
        }
    }
}
