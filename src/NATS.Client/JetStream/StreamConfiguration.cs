// Copyright 2021-2023 The NATS Authors
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

using System;
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
        public CompressionOption CompressionOption { get; }
        public long MaxConsumers { get; }
        public long MaxMsgs { get; }
        public long MaxMsgsPerSubject { get; }
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
        public Republish Republish { get; }
        public SubjectTransform SubjectTransform { get; }
        public ConsumerLimits ConsumerLimits { get; }
        public Mirror Mirror { get; }
        public List<Source> Sources { get; }
        public bool Sealed { get; }
        public bool AllowRollup { get; }
        public bool AllowDirect { get; }
        public bool MirrorDirect { get; }
        public bool DenyDelete { get; }
        public bool DenyPurge { get; }
        public bool DiscardNewPerSubject { get; }
        public IDictionary<string, string> Metadata { get; }
        public ulong FirstSequence { get; private set; }

        [Obsolete("MaxMsgSize was mistakenly renamed in a previous change.", false)]
        public long MaxValueSize => MaxMsgSize;

        internal StreamConfiguration(string json) : this(JSON.Parse(json)) { }
        
        internal StreamConfiguration(JSONNode scNode)
        {
            RetentionPolicy = ApiEnums.GetValueOrDefault(scNode[ApiConstants.Retention].Value,RetentionPolicy.Limits);
            CompressionOption = ApiEnums.GetValueOrDefault(scNode[ApiConstants.Compression].Value,CompressionOption.None);
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
            MaxMsgSize = AsLongOrMinus1(scNode, ApiConstants.MaxMsgSize);
            Replicas = scNode[ApiConstants.NumReplicas].AsInt;
            NoAck = scNode[ApiConstants.NoAck].AsBool;
            TemplateOwner = scNode[ApiConstants.TemplateOwner].Value;
            DuplicateWindow = AsDuration(scNode, ApiConstants.DuplicateWindow, Duration.Zero);
            Placement = Placement.OptionalInstance(scNode[ApiConstants.Placement]);
            Republish = Republish.OptionalInstance(scNode[ApiConstants.Republish]);
            SubjectTransform = SubjectTransform.OptionalInstance(scNode[ApiConstants.SubjectTransform]);
            ConsumerLimits = ConsumerLimits.OptionalInstance(scNode[ApiConstants.ConsumerLimits]);
            Mirror = Mirror.OptionalInstance(scNode[ApiConstants.Mirror]);
            Sources = Source.OptionalListOf(scNode[ApiConstants.Sources]);
            Sealed = scNode[ApiConstants.Sealed].AsBool;
            AllowRollup = scNode[ApiConstants.AllowRollupHdrs].AsBool;
            AllowDirect = scNode[ApiConstants.AllowDirect].AsBool;
            MirrorDirect = scNode[ApiConstants.MirrorDirect].AsBool;
            DenyDelete = scNode[ApiConstants.DenyDelete].AsBool;
            DenyPurge = scNode[ApiConstants.DenyPurge].AsBool;
            DiscardNewPerSubject = scNode[ApiConstants.DiscardNewPerSubject].AsBool;
            Metadata = JsonUtils.StringStringDictionary(scNode, ApiConstants.Metadata);
            FirstSequence = AsUlongOr(scNode, ApiConstants.FirstSequence, 1U);
        }
        
        private StreamConfiguration(StreamConfigurationBuilder builder)
        {
            Name = builder._name;
            Description = builder._description; 
            Subjects = builder._subjects;
            RetentionPolicy = builder._retentionPolicy;
            CompressionOption = builder._compressionOption;
            MaxConsumers = builder._maxConsumers;
            MaxMsgs = builder._maxMsgs;
            MaxMsgsPerSubject = builder._maxMsgsPerSubject;
            MaxBytes = builder._maxBytes;
            MaxAge = builder._maxAge;
            MaxMsgSize = builder._maxMsgSize;
            StorageType = builder._storageType;
            Replicas = builder._replicas;
            NoAck = builder._noAck;
            TemplateOwner = builder._templateOwner;
            DiscardPolicy = builder._discardPolicy;
            DuplicateWindow = builder._duplicateWindow;
            Placement = builder._placement;
            Republish = builder._republish;
            SubjectTransform = builder._subjectTransform;
            ConsumerLimits = builder._consumerLimits;
            Mirror = builder._mirror;
            Sources = builder._sources;
            Sealed = builder._sealed;
            AllowRollup = builder._allowRollup;
            AllowDirect = builder._allowDirect;
            MirrorDirect = builder._mirrorDirect;
            DenyDelete = builder._denyDelete;
            DenyPurge = builder._denyPurge;
            DiscardNewPerSubject = builder._discardNewPerSubject;
            Metadata = builder._metadata;
            FirstSequence = builder._firstSequence;
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject o = new JSONObject();
            AddField(o, ApiConstants.Retention, RetentionPolicy.GetString());
            if (CompressionOption != CompressionOption.None)
            {
                AddField(o, ApiConstants.Compression, CompressionOption.GetString());
            }
            AddField(o, ApiConstants.Storage, StorageType.GetString());
            AddField(o, ApiConstants.Discard, DiscardPolicy.GetString());
            AddField(o, ApiConstants.Name, Name);
            AddField(o, ApiConstants.Description, Description);
            AddField(o, ApiConstants.Subjects, Subjects);
            AddField(o, ApiConstants.MaxConsumers, MaxConsumers);
            AddField(o, ApiConstants.MaxMsgs, MaxMsgs);
            AddField(o, ApiConstants.MaxMsgsPerSubject, MaxMsgsPerSubject);
            AddField(o, ApiConstants.MaxBytes, MaxBytes);
            AddField(o, ApiConstants.MaxAge, MaxAge.Nanos);
            AddField(o, ApiConstants.MaxMsgSize, MaxMsgSize);
            AddField(o, ApiConstants.NumReplicas, Replicas);
            AddField(o, ApiConstants.NoAck, NoAck);
            AddField(o, ApiConstants.TemplateOwner, TemplateOwner);
            AddField(o, ApiConstants.DuplicateWindow, DuplicateWindow.Nanos);
            AddField(o, ApiConstants.Placement, Placement?.ToJsonNode());
            AddField(o, ApiConstants.Republish, Republish?.ToJsonNode());
            AddField(o, ApiConstants.SubjectTransform, SubjectTransform?.ToJsonNode());
            AddField(o, ApiConstants.ConsumerLimits, ConsumerLimits?.ToJsonNode());
            AddField(o, ApiConstants.Mirror, Mirror?.ToJsonNode());
            AddField(o, ApiConstants.Sources, Sources);
            AddField(o, ApiConstants.Sealed, Sealed);
            AddField(o, ApiConstants.AllowRollupHdrs, AllowRollup);
            AddField(o, ApiConstants.AllowDirect, AllowDirect);
            AddField(o, ApiConstants.MirrorDirect, MirrorDirect);
            AddField(o, ApiConstants.DenyDelete, DenyDelete);
            AddField(o, ApiConstants.DenyPurge, DenyPurge);
            AddField(o, ApiConstants.DiscardNewPerSubject, DiscardNewPerSubject);
            AddField(o, ApiConstants.Metadata, Metadata);
            AddFieldWhenGreaterThan(o, ApiConstants.FirstSequence, FirstSequence, 1);
            return o;
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
            internal CompressionOption _compressionOption = CompressionOption.None;
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
            internal Republish _republish;
            internal SubjectTransform _subjectTransform;
            internal ConsumerLimits _consumerLimits;
            internal Mirror _mirror;
            internal readonly List<Source> _sources = new List<Source>();
            internal bool _sealed;
            internal bool _allowRollup;
            internal bool _allowDirect;
            internal bool _mirrorDirect;
            internal bool _denyDelete;
            internal bool _denyPurge;
            internal bool _discardNewPerSubject;
            internal Dictionary<string, string> _metadata = new Dictionary<string, string>();
            internal ulong _firstSequence;

            public StreamConfigurationBuilder() {}
            
            public StreamConfigurationBuilder(StreamConfiguration sc)
            {
                if (sc != null)
                {
                    _name = sc.Name;
                    _description = sc.Description;
                    WithSubjects(sc.Subjects); // handles null
                    _retentionPolicy = sc.RetentionPolicy;
                    _compressionOption = sc.CompressionOption;
                    _maxConsumers = sc.MaxConsumers;
                    _maxMsgs = sc.MaxMsgs;
                    _maxMsgsPerSubject = sc.MaxMsgsPerSubject;
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
                    _republish = sc.Republish;
                    _subjectTransform = sc.SubjectTransform;
                    _consumerLimits = sc.ConsumerLimits;
                    _mirror = sc.Mirror;
                    WithSources(sc.Sources);
                    _sealed = sc.Sealed;
                    _allowRollup = sc.AllowRollup;
                    _allowDirect = sc.AllowDirect;
                    _mirrorDirect = sc.MirrorDirect;
                    _denyDelete = sc.DenyDelete;
                    _denyPurge = sc.DenyPurge;
                    _discardNewPerSubject = sc.DiscardNewPerSubject;
                    _firstSequence = sc.FirstSequence;
                    foreach (string key in sc.Metadata.Keys)
                    {
                        _metadata[key] = sc.Metadata[key];
                    }
                }
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
            /// Sets the compression option in the StreamConfiguration.
            /// </summary>
            /// <param name="option">the compression option of the StreamConfiguration</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithCompressionOption(CompressionOption? option) {
                _compressionOption = option ?? CompressionOption.None;
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
            /// Sets the republish object
            /// </summary>
            /// <param name="republish">the republish object</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithRepublish(Republish republish) {
                _republish = republish;
                return this;
            }

            /// <summary>
            /// Sets the subjectTransform object
            /// </summary>
            /// <param name="subjectTransform">the subjectTransform object</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithSubjectTransform(SubjectTransform subjectTransform) {
                _subjectTransform = subjectTransform;
                return this;
            }

            /// <summary>
            /// Sets the consumerLimits object
            /// </summary>
            /// <param name="consumerLimits">the consumerLimits object</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithConsumerLimits(ConsumerLimits consumerLimits) {
                _consumerLimits = consumerLimits;
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
                if (sources != null) 
                {
                    foreach (Source source in sources) 
                    {
                        if (source != null && !_sources.Contains(source)) 
                        {
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
                if (sources != null) 
                {
                    foreach (Source source in sources) 
                    {
                        if (source != null && !_sources.Contains(source)) 
                        {
                            _sources.Add(source);
                        }
                    }
                }
                return this;
            }

            /// <summary>
            /// Add a source into the StreamConfiguration.
            /// </summary>
            /// <param name="source"></param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder AddSource(Source source) {
                if (source != null && !_sources.Contains(source)) {
                    _sources.Add(source);
                }
                return this;
            }

            /// <summary>
            /// Sets the Allow Rollup mode of the StreamConfiguration.
            /// </summary>
            /// <param name="allowRollup">true to allow rollup.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithAllowRollup(bool allowRollup) {
                _allowRollup = allowRollup;
                return this;
            }

            /// <summary>
            /// Set whether to allow direct message access for a stream
            /// </summary>
            /// <param name="allowDirect">the allow direct setting.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithAllowDirect(bool allowDirect) {
                _allowDirect = allowDirect;
                return this;
            }

            /// <summary>
            /// Set whether to allow unified direct access for mirrors
            /// </summary>
            /// <param name="mirrorDirect">the mirror direct setting.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithMirrorDirect(bool mirrorDirect) {
                _mirrorDirect = mirrorDirect;
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
            /// Set whether discard policy new with max message per subject applies to existing subjects, not just new subjects.
            /// </summary>
            /// <param name="discardNewPerSubject">true to deny purge.</param>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithDiscardNewPerSubject(bool discardNewPerSubject) {
                _discardNewPerSubject = discardNewPerSubject;
                return this;
            }
            
            /// <summary>
            /// Sets the first sequence to be used. 1 is the default. 0 is treated as 1.
            /// </summary>
            /// <param name="firstSequence">specify the first_seq in the stream config when creating the stream.</param>
            /// <returns></returns>
            public StreamConfigurationBuilder WithFirstSequence(ulong firstSequence) {
                _firstSequence = firstSequence == 0 ? 1 : firstSequence;
                return this;
            }
            
            /// <summary>
            /// Set this stream to be sealed. This is irreversible.
            /// </summary>
            /// <returns>The StreamConfigurationBuilder</returns>
            public StreamConfigurationBuilder Seal() {
                _sealed = true;
                return this;
            }

            /// <summary>
            /// Sets the metadata for the configuration 
            /// </summary>
            /// <param name="metadata">the metadata dictionary</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public StreamConfigurationBuilder WithMetadata(IDictionary<string, string> metadata) {
                _metadata.Clear();
                if (metadata != null)
                {
                    foreach (string key in metadata.Keys)
                    {
                        _metadata[key] = metadata[key];
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
                return new StreamConfiguration(this);
            }
        }
    }
}
