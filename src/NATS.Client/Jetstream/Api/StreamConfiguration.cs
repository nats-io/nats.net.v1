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

namespace NATS.Client.Jetstream.Api
{
    public sealed class StreamConfiguration
    {
        public string Name { get; }
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
            Subjects = JsonUtils.StringList(scNode, ApiConstants.Subjects);
            MaxConsumers = scNode[ApiConstants.MaxConsumers].AsLong;
            MaxMsgs = scNode[ApiConstants.MaxMsgs].AsLong;
            MaxBytes = scNode[ApiConstants.MaxBytes].AsLong;
            MaxAge = Duration.OfNanos(scNode[ApiConstants.MaxAge].AsLong);
            MaxMsgSize = scNode[ApiConstants.MaxMsgSize].AsLong;
            Replicas = scNode[ApiConstants.NumReplicas].AsInt;
            NoAck = scNode[ApiConstants.NoAck].AsBool;
            TemplateOwner = scNode[ApiConstants.TemplateOwner].Value;
            DuplicateWindow = Duration.OfNanos(scNode[ApiConstants.DuplicateWindow].AsLong);
            Placement = Placement.OptionalInstance(scNode[ApiConstants.Placement]);
            Mirror = Mirror.OptionalInstance(scNode[ApiConstants.Mirror]);
            Sources = Source.OptionalListOf(scNode[ApiConstants.Sources]);
        }

        private StreamConfiguration(string name, List<string> subjects, RetentionPolicy retentionPolicy, 
            long maxConsumers, long maxMsgs, long maxBytes, Duration maxAge, long maxMsgSize, 
            StorageType storageType, int replicas, bool noAck, string templateOwner, 
            DiscardPolicy discardPolicy, Duration duplicateWindow, Placement placement, Mirror mirror, 
            List<Source> sources)
        {
            Name = name;
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

        internal JSONNode ToJsonNode()
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
                [ApiConstants.Placement] = Placement.ToJsonNode(),
                [ApiConstants.Mirror] = Mirror.ToJsonNode(),
                [ApiConstants.Sources] = sources
            };
        }

        public sealed class Builder
        {
            private string _name;
            private List<string> _subjects = new List<string>();
            private RetentionPolicy _retentionPolicy = Api.RetentionPolicy.Limits;
            private long _maxConsumers = -1;
            private long _maxMsgs = -1;
            private long _maxBytes = -1;
            private Duration _maxAge = Duration.ZERO;
            private long _maxMsgSize = -1;
            private StorageType _storageType = Api.StorageType.File;
            private int _replicas = -1;
            private bool _noAck;
            private string _templateOwner;
            private DiscardPolicy _discardPolicy = Api.DiscardPolicy.Old;
            private Duration _duplicateWindow = Duration.ZERO;
            private Placement _placement;
            private Mirror _mirror;
            private List<Source> _sources = new List<Source>();

            public Builder() {}
            
            public Builder(StreamConfiguration sc)
            {
                _name = sc.Name;
                Subjects(sc.Subjects);
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
                Sources(sc.Sources);
            }

        /// <summary>
        /// Sets the name of the stream.
        /// </summary>
        /// <param name="name">name of the stream.</param>
        public Builder Name(string name) {
            _name = name;
            return this;
        }

        /// <summary>
        /// Sets the subjects in the StreamConfiguration.
        /// </summary>
        /// <param name="subjects">the stream's subjects</param>
        public Builder Subjects(params string[] subjects) {
            _subjects.Clear();
            return AddSubjects(subjects);
        }

        /// <summary>
        /// Sets the subjects in the StreamConfiguration.
        /// </summary>
        /// <param name="subjects">the stream's subjects</param>
        public Builder Subjects(List<string> subjects) {
            _subjects.Clear();
            return AddSubjects(subjects);
        }

        /// <summary>
        /// Sets the subjects in the StreamConfiguration.
        /// </summary>
        /// <param name="subjects">the stream's subjects</param>
        public Builder AddSubjects(params string[] subjects) {
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
        /// Sets the subjects in the StreamConfiguration.
        /// </summary>
        /// <param name="subjects">the stream's subjects</param>
        public Builder AddSubjects(List<string> subjects) {
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
        /// <param name="policy">the retention policy of the StreamConfguration</param>
        public Builder RetentionPolicy(RetentionPolicy? policy) {
            _retentionPolicy = policy ?? Api.RetentionPolicy.Limits;
            return this;
        }

        /// <summary>
        /// Sets the maximum number of consumers in the StreamConfiguration.
        /// </summary>
        /// <param name="maxConsumers">the maximum number of consumers</param>
        public Builder MaxConsumers(long maxConsumers) {
            _maxConsumers = Validator.ValidateMaxConsumers(maxConsumers);
            return this;
        }

        /// <summary>
        /// Sets the maximum number of consumers in the StreamConfiguration.
        /// </summary>
        /// <param name="maxMsgs">the maximum number of messages</param>
        public Builder MaxMessages(long maxMsgs) {
            _maxMsgs = Validator.ValidateMaxMessages(maxMsgs);
            return this;
        }

        /// <summary>
        /// Sets the maximum number of bytes in the StreamConfiguration.
        /// </summary>
        /// <param name="maxBytes">the maximum number of bytes</param>
        public Builder MaxBytes(long maxBytes) {
            _maxBytes = Validator.ValidateMaxBytes(maxBytes);
            return this;
        }

        /// <summary>
        /// Sets the maximum age in the StreamConfiguration.
        /// </summary>
        /// <param name="maxAge">the maximum message age</param>
        public Builder MaxAge(Duration maxAge) {
            _maxAge = Validator.ValidateDurationNotRequiredGtOrEqZero(maxAge);
            return this;
        }

        /// <summary>
        /// Sets the maximum message size in the StreamConfiguration.
        /// </summary>
        /// <param name="maxMsgSize">the maximum message size</param>
        public Builder MaxMsgSize(long maxMsgSize) {
            _maxMsgSize = Validator.ValidateMaxMessageSize(maxMsgSize);
            return this;
        }

        /// <summary>
        /// Sets the storage type in the StreamConfiguration.
        /// </summary>
        /// <param name="storageType">the storage type</param>
        public Builder StorageType(StorageType? storageType) {
            _storageType = storageType?? Api.StorageType.File;
            return this;
        }

        /// <summary>
        /// Sets the number of replicas a message must be stored on in the StreamConfiguration.
        /// </summary>
        /// <param name="replicas">the number of replicas to store this message on</param>
        public Builder Replicas(int replicas) {
            _replicas = Validator.ValidateNumberOfReplicas(replicas);
            return this;
        }

        /// <summary>
        /// Sets the acknowledgement mode of the StreamConfiguration.  if no acknowledgements are
        /// set, then acknowledgements are not sent back to the client.  The default is false.
        /// </summary>
        /// <param name="noAck">true to disable acknowledgements.</param>
        public Builder NoAck(bool noAck) {
            _noAck = noAck;
            return this;
        }

        /// <summary>
        /// Sets the template a stream in the form of raw JSON.
        /// </summary>
        /// <param name="templateOwner">the stream template of the stream.</param>
        public Builder TemplateOwner(string templateOwner) {
            _templateOwner = Validator.EmptyAsNull(templateOwner);
            return this;
        }

        /// <summary>
        /// Sets the discard policy in the StreamConfiguration.
        /// </summary>
        /// <param name="policy">the discard policy of the StreamConfiguration</param>
        public Builder DiscardPolicy(DiscardPolicy? policy) {
            _discardPolicy = policy?? Api.DiscardPolicy.Old;
            return this;
        }

        /// <summary>
        /// Sets the duplicate checking window in the the StreamConfiguration.  A Duration.Zero
        /// disables duplicate checking.  Duplicate checking is disabled by default.
        /// </summary>
        /// <param name="window">duration to hold message ids for duplicate checking.</param>
        public Builder DuplicateWindow(Duration window) {
            _duplicateWindow = Validator.ValidateDurationNotRequiredGtOrEqZero(window);
            return this;
        }

        /// <summary>
        /// Sets the placement directive object
        /// </summary>
        /// <param name="placement">the placement directive object</param>
        public Builder Placement(Placement placement) {
            _placement = placement;
            return this;
        }

        /// <summary>
        /// Sets the mirror  object
        /// </summary>
        /// <param name="mirror">the mirror object</param>
        public Builder Mirror(Mirror mirror) {
            _mirror = mirror;
            return this;
        }

        /// <summary>
        /// Sets the sources in the StreamConfiguration.
        /// </summary>
        /// <param name="sources">the stream's sources</param>
        public Builder Sources(params Source[] sources) {
            _sources.Clear();
            return AddSources(sources);
        }

        /// <summary>
        /// Sets the sources in the StreamConfiguration.
        /// </summary>
        /// <param name="sources">the stream's sources</param>
        public Builder Sources(List<Source> sources) {
            _sources.Clear();
            return AddSources(sources);
        }

        /// <summary>
        /// Sets the sources in the StreamConfiguration.
        /// </summary>
        /// <param name="sources">the stream's sources</param>
        public Builder AddSources(params Source[] sources) {
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
        public Builder AddSources(List<Source> sources) {
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
        public StreamConfiguration Build() {
            return new StreamConfiguration(
                _name,
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
