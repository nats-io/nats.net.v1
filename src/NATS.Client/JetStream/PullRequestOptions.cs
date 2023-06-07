// Copyright 2022-2023 The NATS Authors
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
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// The PullRequestOptions class specifies the options for pull requests
    /// </summary>
    public sealed class PullRequestOptions : JsonSerializable
    {
        /// <summary>
        /// The size of the batch
        /// </summary>
        public int BatchSize { get; }
        
        /// <summary>
        /// The maximum number of bytes in the batch
        /// </summary>
        public long MaxBytes { get; }

        /// <summary>
        /// The no wait flag
        /// </summary>
        public bool NoWait { get; }
        
        /// <summary>
        /// The expires in setting
        /// </summary>
        public Duration ExpiresIn { get; }
        
        /// <summary>
        /// The idle heartbeat time
        /// </summary>
        public Duration IdleHeartbeat { get; }

        private PullRequestOptions(PullRequestOptionsBuilder pro)
        {
            BatchSize = pro._batchSize;
            MaxBytes = pro._maxBytes;
            NoWait = pro._noWait;
            ExpiresIn = pro._expiresIn;
            IdleHeartbeat = pro._idleHeartbeat;
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject {[ApiConstants.Batch] = BatchSize};
            if (MaxBytes > 0)
            {
                jso[ApiConstants.MaxBytes] = MaxBytes;
            }
            if (NoWait)
            {
                jso[ApiConstants.NoWait] = true;
            }
            if (ExpiresIn != null && ExpiresIn.IsPositive())
            {
                jso[ApiConstants.Expires] = ExpiresIn.Nanos;
            }
            if (IdleHeartbeat != null && IdleHeartbeat.IsPositive())
            {
                jso[ApiConstants.IdleHeartbeat] = IdleHeartbeat.Nanos;
            }

            return jso;
        }

        /// <summary>
        /// Creates a builder for the pull options, with batch size since it's always required
        /// </summary>
        /// <param name="batchSize">the size of the batch. Must be greater than 0</param>
        /// <returns>The builder</returns>
        public static PullRequestOptionsBuilder Builder(int batchSize)
        {
            return new PullRequestOptionsBuilder().WithBatchSize(batchSize);
        }

        /// <summary>
        /// The PullRequestOptionsBuilder builds PullRequestOptions
        /// </summary>
        public sealed class PullRequestOptionsBuilder
        {
            internal int _batchSize;
            internal long _maxBytes;
            internal bool _noWait;
            internal Duration _expiresIn;
            internal Duration _idleHeartbeat;
            
            /// <summary>
            /// Set the batch size for the pull
            /// </summary>
            /// <param name="batchSize">The size of the batch. Must be greater than 0</param>
            /// <returns>The builder</returns>
            public PullRequestOptionsBuilder WithBatchSize(int batchSize)
            {
                _batchSize = batchSize;
                return this;
            }
            
            /// <summary>
            /// Set the maximum bytes for the pull
            /// </summary>
            /// <param name="maxBytes">The maximum bytes</param>
            /// <returns>The builder</returns>
            public PullRequestOptionsBuilder WithMaxBytes(long maxBytes)
            {
                _maxBytes = maxBytes;
                return this;
            }
            
            /// <summary>
            /// Set the no wait to true
            /// </summary>
            /// <returns>The builder</returns>
            public PullRequestOptionsBuilder WithNoWait()
            {
                _noWait = true;
                return this;
            }
            
            /// <summary>
            /// Set the no wait flag
            /// </summary>
            /// <param name="noWait">The flag</param>
            /// <returns>The builder</returns>
            public PullRequestOptionsBuilder WithNoWait(bool noWait)
            {
                _noWait = noWait;
                return this;
            }
            
            /// <summary>
            /// Set the expires duration time in millis
            /// </summary>
            /// <param name="expiresInMillis">The millis</param>
            /// <returns>The builder</returns>
            public PullRequestOptionsBuilder WithExpiresIn(long expiresInMillis)
            {
                _expiresIn = Duration.OfMillis(expiresInMillis);
                return this;
            }
            
            /// <summary>
            /// Set the expires duration
            /// </summary>
            /// <param name="expiresIn">The duration</param>
            /// <returns>The builder</returns>
            public PullRequestOptionsBuilder WithExpiresIn(Duration expiresIn)
            {
                _expiresIn = expiresIn;
                return this;
            }
            
            /// <summary>
            /// Set the idle heartbeat time in millis
            /// </summary>
            /// <param name="idleHeartbeatMillis">The millis</param>
            /// <returns>The builder</returns>
            public PullRequestOptionsBuilder WithIdleHeartbeat(long idleHeartbeatMillis)
            {
                _idleHeartbeat = Duration.OfMillis(idleHeartbeatMillis);
                return this;
            }
            
            /// <summary>
            /// Set the idle heartbeat duration
            /// </summary>
            /// <param name="idleHeartbeat">The duration</param>
            /// <returns>The builder</returns>
            public PullRequestOptionsBuilder WithIdleHeartbeat(Duration idleHeartbeat)
            {
                _idleHeartbeat = idleHeartbeat;
                return this;
            }

            /// <summary>
            /// Builds the PullRequestOptions
            /// </summary>
            /// <returns>The PullRequestOptions object.</returns>
            public PullRequestOptions Build() 
            {
                Validator.ValidateGtZero(_batchSize, "Pull batch size");
                return new PullRequestOptions(this);
            }
        }
    }
}
