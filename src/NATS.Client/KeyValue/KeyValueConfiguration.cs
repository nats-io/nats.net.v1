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
using NATS.Client.JetStream;
using static NATS.Client.JetStream.StreamConfiguration;

namespace NATS.Client.KeyValue
{
    public class KeyValueConfiguration
    {
        internal StreamConfiguration BackingConfig { get; }
        
        /// <summary>
        /// The name of the bucket
        /// </summary>
        public string BucketName { get; }

        internal static KeyValueConfiguration Instance(string json) => new KeyValueConfiguration(new StreamConfiguration(json));

        internal KeyValueConfiguration(StreamConfiguration sc)
        {
            BackingConfig = sc;
            BucketName = KeyValueUtil.ExtractBucketName(sc.Name);
        }

        /// <summary>
        /// The description of this bucket
        /// </summary>
        public string Description => BackingConfig.Description;

        /// <summary>
        /// The maximum number of history for any one key. Includes the current value
        /// </summary>
        public long MaxHistoryPerKey => BackingConfig.MaxMsgsPerSubject;

        /// <summary>
        /// The maximum number of bytes for this bucket
        /// </summary>
        public long MaxBucketSize => BackingConfig.MaxBytes;

        /// <summary>
        /// The maximum size for an individual value in the bucket
        /// </summary>
        public long MaxValueSize => BackingConfig.MaxValueSize;

        /// <summary>
        /// The maximum age for a value in this bucket
        /// </summary>
        public long Ttl => BackingConfig.MaxAge.Millis;

        /// <summary>
        /// The storage type for this bucket
        /// </summary>
        public StorageType StorageType => BackingConfig.StorageType;
        
        /// <summary>
        /// The number of replicas for this bucket
        /// </summary>
        public int Replicas => BackingConfig.Replicas;

        /// <summary>
        /// Creates a builder for the Key Value Configuration. 
        /// </summary>
        /// <returns>a stream configuration builder</returns>
        public static KeyValueConfigurationBuilder Builder()
        {
            return new KeyValueConfigurationBuilder();
        }
        
        /// <summary>
        /// Creates a builder for the Key Value Configuration. 
        /// </summary>
        /// <param name="kvc"></param>
        /// <returns>a stream configuration builder</returns>
        public static KeyValueConfigurationBuilder Builder(KeyValueConfiguration kvc)
        {
            return new KeyValueConfigurationBuilder(kvc);
        }

        /// <summary>
        /// KeyValueConfiguration is created using a Builder. The builder supports chaining and will
        /// create a default set of options if no methods are calls.
        /// </summary>
        public sealed class KeyValueConfigurationBuilder
        {
            string _name;
            StreamConfigurationBuilder scBuilder;

            /// <summary>
            /// Default builder
            /// </summary>
            public KeyValueConfigurationBuilder() : this(null) {}

            /// <summary>
            /// Construct the builder by copying another configuration
            /// </summary>
            /// <param name="kvc">the other configuration</param>
            public KeyValueConfigurationBuilder(KeyValueConfiguration kvc) {
                if (kvc == null) {
                    scBuilder = new StreamConfigurationBuilder();
                    WithMaxHistoryPerKey(1);
                    WithReplicas(1);
                }
                else {
                    scBuilder = new StreamConfigurationBuilder(kvc.BackingConfig);
                    _name = KeyValueUtil.ExtractBucketName(kvc.BackingConfig.Name);
                }
            }

            /// <summary>
            /// Sets the name of the store.
            /// </summary>
            /// <param name="name">name of the store</param>
            /// <returns></returns>
            public KeyValueConfigurationBuilder WithName(string name) {
                _name = name;
                return this;
            }

            /// <summary>
            /// Sets the description of the store.
            /// </summary>
            /// <param name="description">the description of the store.</param>
            /// <returns></returns>
            public KeyValueConfigurationBuilder WithDescription(string description) {
                scBuilder.WithDescription(description);
                return this;
            }

            /// <summary>
            /// Sets the maximum number of history for any one key. Includes the current value.
            /// </summary>
            /// <param name="maxHistoryPerKey">the maximum history</param>
            /// <returns></returns>
            public KeyValueConfigurationBuilder WithMaxHistoryPerKey(int maxHistoryPerKey) {
                scBuilder.WithMaxMessagesPerSubject(Validator.ValidateMaxHistory(maxHistoryPerKey));
                return this;
            }

            /// <summary>
            /// Sets the maximum number of bytes in the KeyValueConfiguration. 
            /// </summary>
            /// <param name="maxBucketSize">the maximum number of bytes</param>
            /// <returns></returns>
            public KeyValueConfigurationBuilder WithMaxBucketSize(long maxBucketSize) {
                scBuilder.WithMaxBytes(Validator.ValidateMaxBucketBytes(maxBucketSize));
                return this;
            }

            /// <summary>
            /// Sets the maximum size for an individual value in the KeyValueConfiguration.
            /// </summary>
            /// <param name="maxValueSize">the maximum size for a value</param>
            /// <returns></returns>
            public KeyValueConfigurationBuilder WithMaxValueSize(long maxValueSize) {
                scBuilder.WithMaxMsgSize(Validator.ValidateMaxValueSize(maxValueSize));
                return this;
            }

            /// <summary>
            /// Sets the maximum age for a value in this KeyValueConfiguration
            /// </summary>
            /// <param name="ttl">Sets the maximum age</param>
            /// <returns></returns>
            public KeyValueConfigurationBuilder WithTtl(Duration ttl) {
                scBuilder.WithMaxAge(ttl);
                return this;
            }

            /// <summary>
            /// Sets the storage type in the KeyValueConfiguration.
            /// </summary>
            /// <param name="storageType">the storage type</param>
            /// <returns></returns>
            public KeyValueConfigurationBuilder WithStorageType(StorageType storageType) {
                scBuilder.WithStorageType(storageType);
                return this;
            }

            /// <summary>
            /// Sets the number of replicas a message must be stored on in the KeyValueConfiguration.
            /// </summary>
            /// <param name="replicas"></param>
            /// <returns>the number of replicas</returns>
            public KeyValueConfigurationBuilder WithReplicas(int replicas) {
                scBuilder.WithReplicas(replicas <= 1 ? 1 : replicas);
                return this;
            }

            /// <summary>
            /// Builds the KeyValueConfiguration
            /// </summary>
            /// <returns>the KeyValueConfiguration</returns>
            public KeyValueConfiguration Build() {
                _name = Validator.ValidateKvBucketNameRequired(_name);
                scBuilder.WithName(KeyValueUtil.ToStreamName(_name))
                    .WithSubjects(KeyValueUtil.ToStreamSubject(_name))
                    .WithAllowRollup(true)
                    .WithDiscardPolicy(DiscardPolicy.New)
                    .WithDenyDelete(true);
                return new KeyValueConfiguration(scBuilder.Build());
            }
        }

        public override string ToString()
        {
            return $"BucketName: {BucketName}, Description: {Description}, MaxHistoryPerKey: {MaxHistoryPerKey}, MaxBucketSize: {MaxBucketSize}, MaxValueSize: {MaxValueSize}, Ttl: {Ttl}, StorageType: {StorageType}, Replicas: {Replicas}";
        }
    }
}