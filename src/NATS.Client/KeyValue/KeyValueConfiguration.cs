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
        public StreamConfiguration BackingConfig { get; }
        public string BucketName { get; }

        internal static KeyValueConfiguration Instance(string json) => new KeyValueConfiguration(new StreamConfiguration(json));

        internal KeyValueConfiguration(StreamConfiguration sc)
        {
            BackingConfig = sc;
            BucketName = KeyValueUtil.ExtractBucketName(sc.Name);
        }

        public string Description => BackingConfig.Description;
        public long MaxValues => BackingConfig.MaxMsgs;
        public long MaxHistoryPerKey => BackingConfig.MaxMsgsPerSubject;
        public long MaxBucketSize => BackingConfig.MaxBytes;
        public long MaxValueBytes => BackingConfig.MaxMsgSize;
        public long Ttl => BackingConfig.MaxAge.Millis;
        public StorageType StorageType => BackingConfig.StorageType;
        public int Replicas => BackingConfig.Replicas;

        
        public static KeyValueConfigurationBuilder Builder()
        {
            return new KeyValueConfigurationBuilder();
        }
        
        public static KeyValueConfigurationBuilder Builder(KeyValueConfiguration kvc)
        {
            return new KeyValueConfigurationBuilder(kvc);
        }

        public sealed class KeyValueConfigurationBuilder
        {
            string _name;
            StreamConfigurationBuilder scBuilder;

            public KeyValueConfigurationBuilder() : this(null) {}

            public KeyValueConfigurationBuilder(KeyValueConfiguration kvc) {
                if (kvc == null) {
                    scBuilder = new StreamConfigurationBuilder();
                    WithMaxHistoryPerKey(1);
                }
                else {
                    scBuilder = new StreamConfigurationBuilder(kvc.BackingConfig);
                    _name = KeyValueUtil.ExtractBucketName(kvc.BackingConfig.Name);
                }
            }

            public KeyValueConfigurationBuilder WithName(string name) {
                _name = name;
                return this;
            }

            public KeyValueConfigurationBuilder WithDescription(string description) {
                scBuilder.WithDescription(description);
                return this;
            }

            public KeyValueConfigurationBuilder WithMaxValues(long maxValues) {
                scBuilder.WithMaxMessages(Validator.ValidateMaxBucketValues(maxValues));
                return this;
            }

            public KeyValueConfigurationBuilder WithMaxHistoryPerKey(int maxHistoryPerKey) {
                scBuilder.WithMaxMessagesPerSubject(Validator.ValidateMaxHistory(maxHistoryPerKey));
                return this;
            }

            public KeyValueConfigurationBuilder WithMaxBucketSize(long maxBucketSize) {
                scBuilder.WithMaxBytes(Validator.ValidateMaxBucketBytes(maxBucketSize));
                return this;
            }

            public KeyValueConfigurationBuilder WithMaxValueBytes(long maxValueBytes) {
                scBuilder.WithMaxMsgSize(Validator.ValidateMaxValueBytes(maxValueBytes));
                return this;
            }

            public KeyValueConfigurationBuilder WithTtl(Duration ttl) {
                scBuilder.WithMaxAge(ttl);
                return this;
            }

            public KeyValueConfigurationBuilder WithStorageType(StorageType storageType) {
                scBuilder.WithStorageType(storageType);
                return this;
            }

            public KeyValueConfigurationBuilder WithReplicas(int replicas) {
                scBuilder.WithReplicas(replicas);
                return this;
            }

            public KeyValueConfiguration Build() {
                _name = Validator.ValidateKvBucketNameRequired(_name);
                scBuilder.WithName(KeyValueUtil.ToStreamName(_name))
                    .WithSubjects(KeyValueUtil.ToStreamSubject(_name))
                    .WithAllowRollup(true)
                    .WithDenyDelete(true);
                return new KeyValueConfiguration(scBuilder.Build());
            }
        }
    }
}