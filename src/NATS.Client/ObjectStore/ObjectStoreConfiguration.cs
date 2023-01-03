// Copyright 2022 The NATS Authors
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

namespace NATS.Client.ObjectStore
{
    public class ObjectStoreConfiguration
    {
        internal StreamConfiguration BackingConfig { get; }
        
        /// <summary>
        /// The name of the bucket
        /// </summary>
        public string BucketName { get; }

        internal static ObjectStoreConfiguration Instance(string json) => new ObjectStoreConfiguration(new StreamConfiguration(json));

        internal ObjectStoreConfiguration(StreamConfiguration sc)
        {
            BackingConfig = sc;
            BucketName = ObjectStoreUtil.ExtractBucketName(sc.Name);
        }

        /// <summary>
        /// The description of this bucket
        /// </summary>
        public string Description => BackingConfig.Description;

        /// <summary>
        /// The maximum number of bytes for this bucket
        /// </summary>
        public long MaxBucketSize => BackingConfig.MaxBytes;

        /// <summary>
        /// The maximum age for a value in this bucket
        /// </summary>
        public Duration Ttl => BackingConfig.MaxAge;

        /// <summary>
        /// The storage type for this bucket
        /// </summary>
        public StorageType StorageType => BackingConfig.StorageType;
        
        /// <summary>
        /// The number of replicas for this bucket
        /// </summary>
        public int Replicas => BackingConfig.Replicas;

        /// <summary>
        /// Placement directives to consider when placing replicas of this stream
        /// </summary>
        public Placement Placement => BackingConfig.Placement;

        /// <summary>
        /// Creates a builder for the ObjectStoreConfiguration. 
        /// </summary>
        /// <returns>an ObjectStoreConfiguration builder</returns>
        public static ObjectStoreConfigurationBuilder Builder()
        {
            return new ObjectStoreConfigurationBuilder((ObjectStoreConfiguration)null);
        }

        /// <summary>
        /// Creates a builder for the ObjectStoreConfiguration. 
        /// </summary>
        /// <param name="name">the name of the object store bucket</param>
        /// <returns>an ObjectStoreConfiguration builder</returns>
        public static ObjectStoreConfigurationBuilder Builder(string name)
        {
            return new ObjectStoreConfigurationBuilder(name);
        }
        
        /// <summary>
        /// Creates a builder for the ObjectStoreConfiguration. 
        /// </summary>
        /// <param name="osc">An existing ObjectStoreConfiguration</param>
        /// <returns>an ObjectStoreConfiguration builder</returns>
        public static ObjectStoreConfigurationBuilder Builder(ObjectStoreConfiguration osc)
        {
            return new ObjectStoreConfigurationBuilder(osc);
        }

        /// <summary>
        /// ObjectStoreConfiguration is created using a Builder. The builder supports chaining and will
        /// create a default set of options if no methods are calls.
        /// </summary>
        public sealed class ObjectStoreConfigurationBuilder
        {
            string _name;
            readonly StreamConfigurationBuilder scBuilder;

            /// <summary>
            /// Default builder
            /// </summary>
            public ObjectStoreConfigurationBuilder() : this((ObjectStoreConfiguration)null) {}

            /// <summary>
            /// Builder accepting the object store bucket name.
            /// </summary>
            /// <param name="name">the name of the object store bucket</param>
            public ObjectStoreConfigurationBuilder(string name) : this((ObjectStoreConfiguration)null)
            {
                WithName(name);
            }

            /// <summary>
            /// Construct the builder by copying another configuration
            /// </summary>
            /// <param name="osc">the other configuration</param>
            public ObjectStoreConfigurationBuilder(ObjectStoreConfiguration osc) {
                if (osc == null) {
                    scBuilder = new StreamConfigurationBuilder();
                    WithReplicas(1);
                }
                else {
                    scBuilder = new StreamConfigurationBuilder(osc.BackingConfig);
                    _name = ObjectStoreUtil.ExtractBucketName(osc.BackingConfig.Name);
                }
            }

            /// <summary>
            /// Sets the name of the store.
            /// </summary>
            /// <param name="name">name of the store bucket</param>
            /// <returns></returns>
            public ObjectStoreConfigurationBuilder WithName(string name) {
                _name = Validator.ValidateBucketName(name, true);
                return this;
            }

            /// <summary>
            /// Sets the description of the store.
            /// </summary>
            /// <param name="description">the description of the store.</param>
            /// <returns></returns>
            public ObjectStoreConfigurationBuilder WithDescription(string description) {
                scBuilder.WithDescription(description);
                return this;
            }
            
            /// <summary>
            /// Sets the maximum number of bytes in the ObjectStoreConfiguration. 
            /// </summary>
            /// <param name="maxBucketSize">the maximum number of bytes</param>
            /// <returns></returns>
            public ObjectStoreConfigurationBuilder WithMaxBucketSize(long maxBucketSize) {
                scBuilder.WithMaxBytes(Validator.ValidateMaxBucketBytes(maxBucketSize));
                return this;
            }

            /// <summary>
            /// Sets the maximum age for a value in this ObjectStoreConfiguration
            /// </summary>
            /// <param name="ttl">Sets the maximum age</param>
            /// <returns></returns>
            public ObjectStoreConfigurationBuilder WithTtl(Duration ttl) {
                scBuilder.WithMaxAge(ttl);
                return this;
            }

            /// <summary>
            /// Sets the storage type in the ObjectStoreConfiguration.
            /// </summary>
            /// <param name="storageType">the storage type</param>
            /// <returns></returns>
            public ObjectStoreConfigurationBuilder WithStorageType(StorageType storageType) {
                scBuilder.WithStorageType(storageType);
                return this;
            }

            /// <summary>
            /// Sets the number of replicas a message must be stored on in the ObjectStoreConfiguration.
            /// </summary>
            /// <param name="replicas">number of replicas</param>
            /// <returns>the number of replicas</returns>
            public ObjectStoreConfigurationBuilder WithReplicas(int replicas) {
                scBuilder.WithReplicas(replicas);
                return this;
            }

            /// <summary>
            /// Set the placement directive
            /// </summary>
            /// <param name="placement">the placement</param>
            /// <returns></returns>
            public ObjectStoreConfigurationBuilder WithPlacement(Placement placement) {
                scBuilder.WithPlacement(placement);
                return this;
            }

            /// <summary>
            /// Builds the ObjectStoreConfiguration
            /// </summary>
            /// <returns>the ObjectStoreConfiguration</returns>
            public ObjectStoreConfiguration Build()
            {
                _name = Validator.Required(_name, "name");
                scBuilder.WithName(ObjectStoreUtil.ToStreamName(_name))
                    .WithSubjects(ObjectStoreUtil.ToMetaStreamSubject(_name),
                        ObjectStoreUtil.ToChunkStreamSubject(_name))
                    .WithAllowRollup(true)
                    .WithAllowDirect(true)
                    .WithDiscardPolicy(DiscardPolicy.New);
                return new ObjectStoreConfiguration(scBuilder.Build());
            }
        }

        public override string ToString()
        {
            return $"BucketName: {BucketName}, Description: {Description}, MaxBucketSize: {MaxBucketSize}, Ttl: {Ttl}, StorageType: {StorageType}, Replicas: {Replicas} Placement: {Placement}";
        }
    }
}
