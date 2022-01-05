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
using static NATS.Client.Internals.JsonUtils;

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
        public long MaxMsgsPerSubject { get; }
        public long MaxBytes { get; }
        public Duration MaxAge { get; }
        public long MaxValueSize { get; }
        public StorageType StorageType { get; }
        public int Replicas { get; }
        public bool NoAck { get; }
        public string TemplateOwner { get; }
        public DiscardPolicy DiscardPolicy { get; }
        public Duration DuplicateWindow { get; }
        public Placement Placement { get; }
        public Mirror Mirror { get; }
        public List<Source> Sources { get; }
        private bool Sealed { get; }
        private bool AllowRollup { get; }
        private bool DenyDelete { get; }
        private bool DenyPurge { get; }

        internal StreamConfiguration(string json) : this(JSON.Parse(json)) { }
        
        internal StreamConfiguration(JSONNode scNode)
        {
            RetentionPolicy = ApiEnums.GetValueOrDefault(scNode[ApiConstants.Retention].Value,RetentionPolicy.Limits);
            StorageType = ApiEnums.GetValueOrDefault(scNode[ApiConstants.Storage].Value,StorageType.File);
            DiscardPolicy = ApiEnums.GetValueOrDefault(scNode[ApiConstants.Discard].Value,DiscardPolicy.Old);
            Name = scNode[ApiConstants.Name].Value;
            Description = scNode[ApiConstants.Description].Value;
            Subjects = StringList(scNode, ApiConstants.Subjects);
            MaxConsumers = scNode[ApiConstants.MaxConsumers].AsLong;
            MaxMsgs = AsLongOrMinus1(scNode, ApiConstants.MaxMsgs);
            MaxMsgsPerSubject = AsLongOrMinus1(scNode, ApiConstants.MaxMsgsPerSubject);
            MaxBytes = AsLongOrMinus1(scNode, ApiConstants.MaxBytes);
            MaxAge = AsDuration(scNode, ApiConstants.MaxAge, Duration.Zero);
            MaxValueSize = AsLongOrMinus1(scNode, ApiConstants.MaxMsgSize);
            Replicas = scNode[ApiConstants.NumReplicas].AsInt;
            NoAck = scNode[ApiConstants.NoAck].AsBool;
            TemplateOwner = scNode[ApiConstants.TemplateOwner].Value;
            DuplicateWindow = AsDuration(scNode, ApiConstants.DuplicateWindow, Duration.Zero);
            Placement = Placement.OptionalInstance(scNode[ApiConstants.Placement]);
            Mirror = Mirror.OptionalInstance(scNode[ApiConstants.Mirror]);
            Sources = Source.OptionalListOf(scNode[ApiConstants.Sources]);
            Sealed = scNode[ApiConstants.Sealed].AsBool;
            AllowRollup = scNode[ApiConstants.AllowRollupHdrs].AsBool;
            DenyDelete = scNode[ApiConstants.DenyDelete].AsBool;
            DenyPurge = scNode[ApiConstants.DenyPurge].AsBool;
        }
        
        private StreamConfiguration(StreamConfigurationBuilder builder)
        {
            Name = builder._name;
            Description = builder._description; 
            Subjects = builder._subjects;
            RetentionPolicy = builder._retentionPolicy;
            MaxConsumers = builder._maxConsumers;
            MaxMsgs = builder._maxMsgs;
            MaxMsgsPerSubject = builder._maxMsgsPerSubject;
            MaxBytes = builder._maxBytes;
            MaxAge = builder._maxAge;
            MaxValueSize = builder._maxMsgSize;
            StorageType = builder._storageType;
            Replicas = builder._replicas;
            NoAck = builder._noAck;
            TemplateOwner = builder._templateOwner;
            DiscardPolicy = builder._discardPolicy;
            DuplicateWindow = builder._duplicateWindow;
            Placement = builder._placement;
            Mirror = builder._mirror;
            Sources = builder._sources;
            Sealed = builder._sealed;
            AllowRollup = builder._allowRollup;
            DenyDelete = builder._denyDelete;
            DenyPurge = builder._denyPurge;
        }

        internal override JSONNode ToJsonNode()
        {
            JSONArray sources = new JSONArray();
            if (Sources != null)
            {
                foreach (Source s in Sources)
                {
                    sources.Add(null, s.ToJsonNode());
                }
            }
            return new JSONObject
            {
                [ApiConstants.Retention] = RetentionPolicy.GetString(),
                [ApiConstants.Storage] = StorageType.GetString(),
                [ApiConstants.Discard] = DiscardPolicy.GetString(),
                [ApiConstants.Name] = Name,
                [ApiConstants.Description] = Description,
                [ApiConstants.Subjects] = ToArray(Subjects),
                [ApiConstants.MaxConsumers] = MaxConsumers,
                [ApiConstants.MaxMsgs] = MaxMsgs,
                [ApiConstants.MaxMsgsPerSubject] = MaxMsgsPerSubject,
                [ApiConstants.MaxBytes] = MaxBytes,
                [ApiConstants.MaxAge] = MaxAge.Nanos,
                [ApiConstants.MaxMsgSize] = MaxValueSize,
                [ApiConstants.NumReplicas] = Replicas,
                [ApiConstants.NoAck] = NoAck,
                [ApiConstants.TemplateOwner] = TemplateOwner,
                [ApiConstants.DuplicateWindow] = DuplicateWindow.Nanos,
                [ApiConstants.Placement] = Placement?.ToJsonNode(),
                [ApiConstants.Mirror] = Mirror?.ToJsonNode(),
                [ApiConstants.Sources] = sources,
                // never write sealed
                [ApiConstants.AllowRollupHdrs] = AllowRollup,
                [ApiConstants.DenyDelete] = DenyDelete,
                [ApiConstants.DenyPurge] = DenyPurge
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
            internal string _name;
            internal string _description;
            internal readonly List<string> _subjects = new List<string>();
            internal RetentionPolicy _retentionPolicy = RetentionPolicy.Limits;
            internal long _maxConsumers = -1;
            internal long _maxMsgs = -1;
            internal long _maxMsgsPerSubject = -1;
            internal long _maxBytes = -1;
            internal Duration _maxAge = Duration.Zero;
            internal long _maxMsgSize = -1;
            internal StorageType _storageType = StorageType.File;
            internal int _replicas = 1;
            internal bool _noAck;
            internal string _templateOwner;
            internal DiscardPolicy _discardPolicy = DiscardPolicy.Old;
            internal Duration _duplicateWindow = Duration.Zero;
            internal Placement _placement;
            internal Mirror _mirror;
            internal readonly List<Source> _sources = new List<Source>();
            internal bool _sealed;
            internal bool _allowRollup;
            internal bool _denyDelete;
            internal bool _denyPurge;

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
                _maxMsgsPerSubject = sc.MaxMsgsPerSubject;
                _maxBytes = sc.MaxBytes;
                _maxAge = sc.MaxAge;
                _maxMsgSize = sc.MaxValueSize;
                _storageType = sc.StorageType;
                _replicas = sc.Replicas;
                _noAck = sc.NoAck;
                _templateOwner = sc.TemplateOwner;
                _discardPolicy = sc.DiscardPolicy;
                _duplicateWindow = sc.DuplicateWindow;
                _placement = sc.Placement;
                _mirror = sc.Mirror;
                WithSources(sc.Sources);
                _sealed = sc.Sealed;
                _allowRollup = sc.AllowRollup;
                _denyDelete = sc.DenyDelete;
                _denyPurge = sc.DenyPurge;
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
            /// Sets the maximum number of messages in the StreamConfiguration.
            /// </summary>
            /// <param name="maxMsgs">the maximum number of messages</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithMaxMessages(long maxMsgs) {
                _maxMsgs = Validator.ValidateMaxMessages(maxMsgs);
                return this;
            }

            /// <summary>
            /// Sets the maximum number of message per subject in the StreamConfiguration.
            /// </summary>
            /// <param name="maxMsgsPerSubject">the maximum number of messages</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithMaxMessagesPerSubject(long maxMsgsPerSubject) {
                _maxMsgsPerSubject = Validator.ValidateMaxMessagesPerSubject(maxMsgsPerSubject);
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
            /// Sets the Allow Rollup mode of the StreamConfiguration.
            /// </summary>
            /// <param name="allowRollup">true to allow rollup headers.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithAllowRollup(bool allowRollup) {
                _allowRollup = allowRollup;
                return this;
            }

            /// <summary>
            /// Sets the Deny Delete mode of the StreamConfiguration.
            /// </summary>
            /// <param name="denyDelete">true to deny delete.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithDenyDelete(bool denyDelete) {
                _denyDelete = denyDelete;
                return this;
            }

            /// <summary>
            /// Sets the Deny Purge mode of the StreamConfiguration.
            /// </summary>
            /// <param name="denyPurge">true to deny purge.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithDenyPurge(bool denyPurge) {
                _denyPurge = denyPurge;
                return this;
            }

            /// <summary>
            /// Builds the ConsumerConfiguration
            /// </summary>
            /// <returns>The StreamConfiguration</returns>
            public StreamConfiguration Build()
            {
                return new StreamConfiguration(this);
            }
        }
    }
}
