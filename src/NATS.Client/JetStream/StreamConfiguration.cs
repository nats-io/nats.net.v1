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
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class StreamConfiguration : JsonSerializable
    {
        public string Name { get; }
        public string Description { get; }
        public List<string> Subjects { get; }
        public RetentionPolicy RetentionPolicy { get; }
        public long MaxConsumers { get; }
        public long MaxMsgs { get; }
        public long MaxBytes { get; }
        public Duration MaxAge { get; }
        public long MaxMsgSize { get; }
        public StorageType StorageType { get; }
        public int Replicas { get; }
        public bool NoAck { get; }
        public string TemplateOwner { get; }
        public DiscardPolicy DiscardPolicy { get; }
        public Duration DuplicateWindow { get; }
        public Placement Placement { get; }
        public Mirror Mirror { get; }
        public List<Source> Sources { get; }

        internal StreamConfiguration(string json) : this(JSON.Parse(json)) { }
        
        internal StreamConfiguration(JSONNode scNode)
        {
            RetentionPolicy = ApiEnums.GetValueOrDefault(scNode[ApiConstants.Retention].Value,RetentionPolicy.Limits);
            StorageType = ApiEnums.GetValueOrDefault(scNode[ApiConstants.Storage].Value,StorageType.File);
            DiscardPolicy = ApiEnums.GetValueOrDefault(scNode[ApiConstants.Discard].Value,DiscardPolicy.Old);
            Name = scNode[ApiConstants.Name].Value;
            Description = scNode[ApiConstants.Description].Value;
            Subjects = JsonUtils.StringList(scNode, ApiConstants.Subjects);
            MaxConsumers = scNode[ApiConstants.MaxConsumers].AsLong;
            MaxMsgs = JsonUtils.AsLongOrMinus1(scNode, ApiConstants.MaxMsgs);
            MaxBytes = JsonUtils.AsLongOrMinus1(scNode, ApiConstants.MaxBytes);
            MaxAge = JsonUtils.AsDuration(scNode, ApiConstants.MaxAge, Duration.Zero);
            MaxMsgSize = JsonUtils.AsLongOrMinus1(scNode, ApiConstants.MaxMsgSize);
            Replicas = scNode[ApiConstants.NumReplicas].AsInt;
            NoAck = scNode[ApiConstants.NoAck].AsBool;
            TemplateOwner = scNode[ApiConstants.TemplateOwner].Value;
            DuplicateWindow = JsonUtils.AsDuration(scNode, ApiConstants.DuplicateWindow, Duration.Zero);
            Placement = Placement.OptionalInstance(scNode[ApiConstants.Placement]);
            Mirror = Mirror.OptionalInstance(scNode[ApiConstants.Mirror]);
            Sources = Source.OptionalListOf(scNode[ApiConstants.Sources]);
        }
        
        private StreamConfiguration(string name, string description, List<string> subjects, RetentionPolicy retentionPolicy, 
            long maxConsumers, long maxMsgs, long maxBytes, Duration maxAge, long maxMsgSize, 
            StorageType storageType, int replicas, bool noAck, string templateOwner, 
            DiscardPolicy discardPolicy, Duration duplicateWindow, Placement placement, Mirror mirror, 
            List<Source> sources)
        {
            Name = name;
            Description = description; 
            Subjects = subjects;
            RetentionPolicy = retentionPolicy;
            MaxConsumers = maxConsumers;
            MaxMsgs = maxMsgs;
            MaxBytes = maxBytes;
            MaxAge = maxAge;
            MaxMsgSize = maxMsgSize;
            StorageType = storageType;
            Replicas = replicas;
            NoAck = noAck;
            TemplateOwner = templateOwner;
            DiscardPolicy = discardPolicy;
            DuplicateWindow = duplicateWindow;
            Placement = placement;
            Mirror = mirror;
            Sources = sources;
        }

        internal override JSONNode ToJsonNode()
        {
            JSONArray sources = new JSONArray();
            foreach (Source s in Sources)
            {
                sources.Add(null, s.ToJsonNode());
            }
            return new JSONObject
            {
                [ApiConstants.Retention] = RetentionPolicy.GetString(),
                [ApiConstants.Storage] = StorageType.GetString(),
                [ApiConstants.Discard] = DiscardPolicy.GetString(),
                [ApiConstants.Name] = Name,
                [ApiConstants.Description] = Description,
                [ApiConstants.Subjects] = JsonUtils.ToArray(Subjects),
                [ApiConstants.MaxConsumers] = MaxConsumers,
                [ApiConstants.MaxMsgs] = MaxMsgs,
                [ApiConstants.MaxBytes] = MaxBytes,
                [ApiConstants.MaxAge] = MaxAge.Nanos,
                [ApiConstants.MaxMsgSize] = MaxMsgSize,
                [ApiConstants.NumReplicas] = Replicas,
                [ApiConstants.NoAck] = NoAck,
                [ApiConstants.TemplateOwner] = TemplateOwner,
                [ApiConstants.DuplicateWindow] = DuplicateWindow.Nanos,
                [ApiConstants.Placement] = Placement?.ToJsonNode(),
                [ApiConstants.Mirror] = Mirror?.ToJsonNode(),
                [ApiConstants.Sources] = sources
            };
        }

        public static StreamConfigurationBuilder Builder()
        {
            return new StreamConfigurationBuilder();
        }

        public static StreamConfigurationBuilder Builder(StreamConfiguration sc)
        {
            return new StreamConfigurationBuilder(sc);
        }

        public sealed class StreamConfigurationBuilder
        {
            private string _name;
            private string _description;
            private readonly List<string> _subjects = new List<string>();
            private RetentionPolicy _retentionPolicy = RetentionPolicy.Limits;
            private long _maxConsumers = -1;
            private long _maxMsgs = -1;
            private long _maxBytes = -1;
            private Duration _maxAge = Duration.Zero;
            private long _maxMsgSize = -1;
            private StorageType _storageType = StorageType.File;
            private int _replicas = 1;
            private bool _noAck;
            private string _templateOwner;
            private DiscardPolicy _discardPolicy = DiscardPolicy.Old;
            private Duration _duplicateWindow = Duration.Zero;
            private Placement _placement;
            private Mirror _mirror;
            private readonly List<Source> _sources = new List<Source>();

            public StreamConfigurationBuilder() {}
            
            public StreamConfigurationBuilder(StreamConfiguration sc)
            {
                if (sc == null) return;
                _name = sc.Name;
                _description = sc.Description;
                WithSubjects(sc.Subjects); // handles null
                _retentionPolicy = sc.RetentionPolicy;
                _maxConsumers = sc.MaxConsumers;
                _maxMsgs = sc.MaxMsgs;
                _maxBytes = sc.MaxBytes;
                _maxAge = sc.MaxAge;
                _maxMsgSize = sc.MaxMsgSize;
                _storageType = sc.StorageType;
                _replicas = sc.Replicas;
                _noAck = sc.NoAck;
                _templateOwner = sc.TemplateOwner;
                _discardPolicy = sc.DiscardPolicy;
                _duplicateWindow = sc.DuplicateWindow;
                _placement = sc.Placement;
                _mirror = sc.Mirror;
                WithSources(sc.Sources);
            }

            /// <summary>
            /// Sets the name of the stream.
            /// </summary>
            /// <param name="name">name of the stream.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithName(string name) {
                _name = Validator.ValidateStreamName(name, false);
                return this;
            }

            /// <summary>
            /// Sets the description.
            /// </summary>
            /// <param name="description">the description</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithDescription(string description)
            {
                _description = description;
                return this;
            }

            /// <summary>
            /// Sets the subjects in the StreamConfiguration.
            /// </summary>
            /// <param name="subjects">the stream's subjects</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithSubjects(params string[] subjects) {
                _subjects.Clear();
                return AddSubjects(subjects);
            }

            /// <summary>
            /// Sets the subjects in the StreamConfiguration.
            /// </summary>
            /// <param name="subjects">the stream's subjects</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithSubjects(List<string> subjects) {
                _subjects.Clear();
                return AddSubjects(subjects);
            }

            /// <summary>
            /// Add the subjects in the StreamConfiguration.
            /// </summary>
            /// <param name="subjects">the stream's subjects</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder AddSubjects(params string[] subjects) {
                if (subjects != null) {
                    foreach (string sub in subjects) {
                        if (sub != null && !_subjects.Contains(sub)) {
                            _subjects.Add(sub);
                        }
                    }
                }
                return this;
            }

            /// <summary>
            /// Add the subjects in the StreamConfiguration.
            /// </summary>
            /// <param name="subjects">the stream's subjects</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder AddSubjects(List<string> subjects) {
                if (subjects != null) {
                    foreach (string sub in subjects) {
                        if (sub != null && !_subjects.Contains(sub)) {
                            _subjects.Add(sub);
                        }
                    }
                }
                return this;
            }

            /// <summary>
            /// Sets the retention policy in the StreamConfiguration.
            /// </summary>
            /// <param name="policy">the retention policy of the StreamConfiguration</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithRetentionPolicy(RetentionPolicy? policy) {
                _retentionPolicy = policy ?? RetentionPolicy.Limits;
                return this;
            }

            /// <summary>
            /// Sets the maximum number of consumers in the StreamConfiguration.
            /// </summary>
            /// <param name="maxConsumers">the maximum number of consumers</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithMaxConsumers(long maxConsumers) 
            {
                _maxConsumers = Validator.ValidateMaxConsumers(maxConsumers);
                return this;
            }

            /// <summary>
            /// Sets the maximum number of consumers in the StreamConfiguration.
            /// </summary>
            /// <param name="maxMsgs">the maximum number of messages</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithMaxMessages(long maxMsgs) {
                _maxMsgs = Validator.ValidateMaxMessages(maxMsgs);
                return this;
            }

            /// <summary>
            /// Sets the maximum number of bytes in the StreamConfiguration.
            /// </summary>
            /// <param name="maxBytes">the maximum number of bytes</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithMaxBytes(long maxBytes) {
                _maxBytes = Validator.ValidateMaxBytes(maxBytes);
                return this;
            }

            /// <summary>
            /// Sets the maximum age in the StreamConfiguration.
            /// </summary>
            /// <param name="maxAge">the maximum message age as a Duration</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithMaxAge(Duration maxAge) 
            {
                _maxAge = Validator.ValidateDurationNotRequiredGtOrEqZero(maxAge);
                return this;
            }

            /// <summary>
            /// Sets the maximum age in the StreamConfiguration.
            /// </summary>
            /// <param name="maxAgeMillis">the maximum message age as millis</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithMaxAge(long maxAgeMillis) {
                _maxAge = Validator.ValidateDurationNotRequiredGtOrEqZero(maxAgeMillis);
                return this;
            }

            /// <summary>
            /// Sets the maximum message size in the StreamConfiguration.
            /// </summary>
            /// <param name="maxMsgSize">the maximum message size</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithMaxMsgSize(long maxMsgSize) {
                _maxMsgSize = Validator.ValidateMaxMessageSize(maxMsgSize);
                return this;
            }

            /// <summary>
            /// Sets the storage type in the StreamConfiguration.
            /// </summary>
            /// <param name="storageType">the storage type</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithStorageType(StorageType? storageType) {
                _storageType = storageType ?? StorageType.File;
                return this;
            }

            /// <summary>
            /// Sets the number of replicas a message must be stored on in the StreamConfiguration.
            /// </summary>
            /// <param name="replicas">the number of replicas to store this message on</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithReplicas(int replicas) {
                _replicas = Validator.ValidateNumberOfReplicas(replicas);
                return this;
            }

            /// <summary>
            /// Sets the acknowledgement mode of the StreamConfiguration.  if no acknowledgements are
            /// set, then acknowledgements are not sent back to the client.  The default is false.
            /// </summary>
            /// <param name="noAck">true to disable acknowledgements.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithNoAck(bool noAck) {
                _noAck = noAck;
                return this;
            }

            /// <summary>
            /// Sets the template a stream in the form of raw JSON.
            /// </summary>
            /// <param name="templateOwner">the stream template of the stream.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithTemplateOwner(string templateOwner) {
                _templateOwner = Validator.EmptyAsNull(templateOwner);
                return this;
            }

            /// <summary>
            /// Sets the discard policy in the StreamConfiguration.
            /// </summary>
            /// <param name="policy">the discard policy of the StreamConfiguration</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithDiscardPolicy(DiscardPolicy? policy) {
                _discardPolicy = policy ?? DiscardPolicy.Old;
                return this;
            }

            /// <summary>
            /// Sets the duplicate checking window in the the StreamConfiguration.  A Duration.Zero
            /// disables duplicate checking.  Duplicate checking is disabled by default.
            /// </summary>
            /// <param name="window">duration to hold message ids for duplicate checking.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithDuplicateWindow(Duration window) {
                _duplicateWindow = Validator.ValidateDurationNotRequiredGtOrEqZero(window);
                return this;
            }

            /// <summary>
            /// Sets the duplicate checking window in the the StreamConfiguration.  A Duration.Zero
            /// disables duplicate checking.  Duplicate checking is disabled by default.
            /// </summary>
            /// <param name="windowMillis">duration to hold message ids for duplicate checking.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithDuplicateWindow(long windowMillis) {
                _duplicateWindow = Validator.ValidateDurationNotRequiredGtOrEqZero(windowMillis);
                return this;
            }

            /// <summary>
            /// Sets the placement directive object
            /// </summary>
            /// <param name="placement">the placement directive object</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithPlacement(Placement placement) {
                _placement = placement;
                return this;
            }

            /// <summary>
            /// Sets the mirror  object
            /// </summary>
            /// <param name="mirror">the mirror object</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithMirror(Mirror mirror) {
                _mirror = mirror;
                return this;
            }

            /// <summary>
            /// Sets the sources in the StreamConfiguration.
            /// </summary>
            /// <param name="sources">the stream's sources</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithSources(params Source[] sources) {
                _sources.Clear();
                return AddSources(sources);
            }

            /// <summary>
            /// Sets the sources in the StreamConfiguration.
            /// </summary>
            /// <param name="sources">the stream's sources</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithSources(List<Source> sources) {
                _sources.Clear();
                return AddSources(sources);
            }

            /// <summary>
            /// Sets the sources in the StreamConfiguration.
            /// </summary>
            /// <param name="sources">the stream's sources</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder AddSources(params Source[] sources) {
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
            /// Sets the sources in the StreamConfiguration.
            /// </summary>
            /// <param name="sources">the stream's sources</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder AddSources(List<Source> sources) {
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
            /// Builds the ConsumerConfiguration
            /// </summary>
            /// <returns>The StreamConfiguration</returns>
            public StreamConfiguration Build() 
            {
                return new StreamConfiguration(
                    _name,
                    _description,
                    _subjects,
                    _retentionPolicy,
                    _maxConsumers,
                    _maxMsgs,
                    _maxBytes,
                    _maxAge,
                    _maxMsgSize,
                    _storageType,
                    _replicas,
                    _noAck,
                    _templateOwner,
                    _discardPolicy,
                    _duplicateWindow,
                    _placement,
                    _mirror,
                    _sources
                );
            }
        }
    }
}
