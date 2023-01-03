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

using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using static NATS.Client.JetStream.StreamConfiguration;
using static NATS.Client.KeyValue.KeyValueUtil;

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
            BucketName = ExtractBucketName(sc.Name);
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
        public long MaxValueSize => BackingConfig.MaxMsgSize;

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
        /// Republish options
        /// </summary>
        public Republish Republish => BackingConfig.Republish;

        /// <summary>
        /// Allow Direct setting
        /// </summary>
        public bool AllowDirect => BackingConfig.AllowDirect;
            
        /// <summary>
        /// Creates a builder for the KeyValueConfiguration. 
        /// </summary>
        /// <returns>a KeyValueConfiguration builder</returns>
        public static KeyValueConfigurationBuilder Builder()
        {
            return new KeyValueConfigurationBuilder((KeyValueConfiguration)null);
        }
            
        /// <summary>
        /// Creates a builder for the KeyValueConfiguration. 
        /// </summary>
        /// <param name="name">name of the key value bucket</param>
        /// <returns>a KeyValueConfiguration builder</returns>
        public static KeyValueConfigurationBuilder Builder(string name)
        {
            return new KeyValueConfigurationBuilder(name);
        }
        
        /// <summary>
        /// Creates a builder for the KeyValueConfiguration. 
        /// </summary>
        /// <param name="kvc"></param>
        /// <returns>a KeyValueConfiguration builder</returns>
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
            Mirror _mirror;
            readonly List<Source> _sources = new List<Source>();
            readonly StreamConfigurationBuilder scBuilder;

            /// <summary>
            /// Default builder
            /// </summary>
            public KeyValueConfigurationBuilder() : this((KeyValueConfiguration)null) {}

            /// <summary>
            /// Builder accepting the object store bucket name.
            /// </summary>
            /// <param name="name">name of the key value bucket</param>
            public KeyValueConfigurationBuilder(string name) : this((KeyValueConfiguration)null)
            {
                WithName(name);
            }

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
                    _name = ExtractBucketName(kvc.BackingConfig.Name);
                }
            }

            /// <summary>
            /// Sets the name of the store.
            /// </summary>
            /// <param name="name">name of the key value bucket</param>
            /// <returns></returns>
            public KeyValueConfigurationBuilder WithName(string name) {
                _name = Validator.ValidateBucketName(name, true);
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
            /// <param name="replicas">number of replicas</param>
            /// <returns>the number of replicas</returns>
            public KeyValueConfigurationBuilder WithReplicas(int replicas) {
                scBuilder.WithReplicas(replicas);
                return this;
            }

            /// <summary>
            /// Set the placement directive
            /// </summary>
            /// <param name="placement">the placement</param>
            /// <returns></returns>
            public KeyValueConfigurationBuilder WithPlacement(Placement placement) {
                scBuilder.WithPlacement(placement);
                return this;
            }

            /// <summary>
            /// Set the republish options
            /// </summary>
            /// <param name="republish">the republish</param>
            /// <returns>The KeyValueConfigurationBuilder</returns>
            public KeyValueConfigurationBuilder WithRepublish(Republish republish) {
                scBuilder.WithRepublish(republish);
                return this;
            }

            /// <summary>
            /// Sets the mirror in the KeyValueConfiguration.
            /// </summary>
            /// <param name="mirror">the mirror</param>
            /// <returns>The KeyValueConfigurationBuilder</returns>
            public KeyValueConfigurationBuilder WithMirror(Mirror mirror)
            {
                _mirror = mirror;
                return this;
            }

            /// <summary>
            /// Sets the sources in the KeyValueConfiguration.
            /// </summary>
            /// <param name="sources">the KeyValue's sources</param>
            /// <returns>The KeyValueConfigurationBuilder</returns>
            public KeyValueConfigurationBuilder WithSources(params Source[] sources) {
                _sources.Clear();
                return AddSources(sources);
            }

            /// <summary>
            /// Sets the sources in the KeyValueConfiguration.
            /// </summary>
            /// <param name="sources">the KeyValue's sources</param>
            /// <returns>The KeyValueConfigurationBuilder</returns>
            public KeyValueConfigurationBuilder WithSources(List<Source> sources) {
                _sources.Clear();
                return AddSources(sources);
            }

            /// <summary>
            /// Sets the sources in the KeyValueConfiguration.
            /// </summary>
            /// <param name="sources">the KeyValue's sources</param>
            /// <returns>The KeyValueConfigurationBuilder</returns>
            public KeyValueConfigurationBuilder AddSources(params Source[] sources) {
                if (sources != null) {
                    foreach (Source source in sources) {
                        if (source != null && !_sources.Contains(source)) {
                            _sources.Add(source);
                        }
                    }
                }
                return this;
            }

            /// <summary>
            /// Sets the sources in the KeyValueConfiguration.
            /// </summary>
            /// <param name="sources">the KeyValue's sources</param>
            /// <returns>The KeyValueConfigurationBuilder</returns>
            public KeyValueConfigurationBuilder AddSources(List<Source> sources) {
                if (sources != null) {
                    foreach (Source source in sources) {
                        if (source != null && !_sources.Contains(source)) {
                            _sources.Add(source);
                        }
                    }
                }
                return this;
            }

            /// <summary>
            /// Add a source into the KeyValueConfiguration.
            /// </summary>
            /// <param name="source"></param>
            /// <returns>The KeyValueConfigurationBuilder</returns>
            public KeyValueConfigurationBuilder AddSource(Source source) {
                if (source != null && !_sources.Contains(source)) {
                    _sources.Add(source);
                }
                return this;
            }

            /// <summary>
            /// Set whether to allow direct message access.
            /// This is an optimization for Key Value since
            /// but is not available on all account / jwt configuration.
            /// </summary>
            /// <param name="allowDirect">true to allow direct headers.</param>
            /// <returns>The KeyValueConfigurationBuilder</returns>
            public KeyValueConfigurationBuilder WithAllowDirect(bool allowDirect) {
                scBuilder.WithAllowDirect(allowDirect);
                return this;
            }

            /// <summary>
            /// Builds the KeyValueConfiguration
            /// </summary>
            /// <returns>the KeyValueConfiguration</returns>
            public KeyValueConfiguration Build() {
                _name = Validator.Required(_name, "name");
                scBuilder.WithName(ToStreamName(_name))
                    .WithAllowRollup(true)
                    .WithDiscardPolicy(DiscardPolicy.New)
                    .WithDenyDelete(true);
                
                if (_mirror != null) {
                    scBuilder.WithMirrorDirect(true);
                    string name = _mirror.Name;
                    if (HasPrefix(name)) {
                        scBuilder.WithMirror(_mirror);
                    }
                    else {
                        scBuilder.WithMirror(
                            Mirror.Builder(_mirror)
                                .WithName(ToStreamName(name))
                                .Build());
                    }
                }
                else if (_sources.Count > 0) {
                    foreach (Source source in _sources) {
                        string name = source.Name;
                        if (HasPrefix(name)) {
                            scBuilder.AddSource(source);
                        }
                        else {
                            scBuilder.AddSource(
                                Source.Builder(source)
                                    .WithName(ToStreamName(name))
                                    .Build());
                        }
                    }
                }
                else {
                    scBuilder.WithSubjects(ToStreamSubject(_name));
                }
                
                return new KeyValueConfiguration(scBuilder.Build());
            }
        }

        public override string ToString()
        {
            return $"BucketName: {BucketName}, Description: {Description}, MaxHistoryPerKey: {MaxHistoryPerKey}, MaxBucketSize: {MaxBucketSize}, MaxValueSize: {MaxValueSize}, Ttl: {Ttl}, StorageType: {StorageType}, Replicas: {Replicas} Placement: {Placement}, Republish: {Republish}";
        }
    }
}
