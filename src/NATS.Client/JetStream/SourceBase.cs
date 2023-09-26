using System;
using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Information about an upstream stream source or a mirror
    /// </summary>
    public class SourceBase : JsonSerializable
    {
        /// <summary>
        /// Source stream name.
        /// </summary>
        public string Name { get; }
        
        /// <summary>
        /// The sequence to start replicating from.
        /// </summary>
        public ulong StartSeq { get; }
        
        /// <summary>
        /// The time stamp to start replicating from.
        /// </summary>
        public DateTime StartTime { get; }
        
        /// <summary>
        /// The subject filter to replicate
        /// </summary>
        public string FilterSubject { get; }

        /// <summary>
        /// External stream reference
        /// </summary>
        public External External { get; }

        /// <summary>
        /// The subject transforms
        /// </summary>
        public IList<SubjectTransform> SubjectTransforms { get; }

        internal SourceBase(JSONNode sourceBaseNode)
        {
            Name = sourceBaseNode[ApiConstants.Name].Value;
            StartSeq = sourceBaseNode[ApiConstants.OptStartSeq].AsUlong;
            StartTime = JsonUtils.AsDate(sourceBaseNode[ApiConstants.OptStartTime]);
            FilterSubject = sourceBaseNode[ApiConstants.FilterSubject].Value;
            External = External.OptionalInstance(sourceBaseNode[ApiConstants.External]);
            SubjectTransforms = SubjectTransform.OptionalListOf(sourceBaseNode[ApiConstants.SubjectTransforms]);
        }

        protected SourceBase(string name, ulong startSeq, DateTime startTime, string filterSubject, External external,
            IList<SubjectTransform> subjectTransforms = null)
        {
            Name = name;
            StartSeq = startSeq;
            StartTime = startTime;
            FilterSubject = filterSubject;
            External = external;
            SubjectTransforms = subjectTransforms;
        }
 
        protected SourceBase(ISourceBaseBuilder isbb)
        {
            Name = isbb.Name;
            StartSeq = isbb.StartSeq;
            StartTime = isbb.StartTime;
            FilterSubject = isbb.FilterSubject;
            External = isbb.External;
            SubjectTransforms = isbb.SubjectTransforms;
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.OptStartSeq, StartSeq);
            JsonUtils.AddField(jso, ApiConstants.OptStartTime, JsonUtils.ToString(StartTime));
            JsonUtils.AddField(jso, ApiConstants.FilterSubject, FilterSubject);
            JsonUtils.AddField(jso, ApiConstants.External, External);
            JsonUtils.AddField(jso, ApiConstants.SubjectTransforms, SubjectTransforms);
            return jso;
        }
                
        public interface ISourceBaseBuilder
        {
            string Name { get; }
            ulong StartSeq { get; }
            DateTime StartTime { get; }
            string FilterSubject { get; }
            External External { get; }
            IList<SubjectTransform> SubjectTransforms { get; }
        }

        public abstract class SourceBaseBuilder<TBuilder, TSourceBase> : ISourceBaseBuilder
        {
            protected string _name;
            protected ulong _startSeq;
            protected DateTime _startTime;
            protected string _filterSubject;
            protected External _external;
            protected IList<SubjectTransform> _subjectTransforms;

            public string Name => _name;
            public ulong StartSeq => _startSeq;
            public DateTime StartTime => _startTime;
            public string FilterSubject => _filterSubject;
            public External External => _external;
            public IList<SubjectTransform> SubjectTransforms => _subjectTransforms;
            
            protected abstract TBuilder GetThis();

            protected SourceBaseBuilder() { }

            protected SourceBaseBuilder(SourceBase sourceBase)
            {
                _name = sourceBase.Name;
                _startSeq = sourceBase.StartSeq;
                _startTime = sourceBase.StartTime;
                _filterSubject = sourceBase.FilterSubject;
                _external = sourceBase.External;
                _subjectTransforms = sourceBase.SubjectTransforms;
            }

            /// <summary>
            /// Set the source name.
            /// </summary>
            /// <param name="name">the name</param>
            /// <returns>The Builder</returns>
            public TBuilder WithName(string name)
            {
                _name = name;
                return GetThis();
            }

            /// <summary>
            /// Set the start sequence.
            /// </summary>
            /// <param name="startSeq">the start sequence</param>
            /// <returns>The Builder</returns>
            public TBuilder WithStartSeq(ulong startSeq)
            {
                _startSeq = startSeq;
                return GetThis();
            }

            /// <summary>
            /// Set the start time.
            /// </summary>
            /// <param name="startTime">the start time</param>
            /// <returns>The Builder</returns>
            public TBuilder WithStartTime(DateTime startTime)
            {
                _startTime = startTime;
                return GetThis();
            }

            /// <summary>
            /// Set the filter subject.
            /// </summary>
            /// <param name="filterSubject">the filterSubject</param>
            /// <returns>The Builder</returns>
            public TBuilder WithFilterSubject(string filterSubject)
            {
                _filterSubject = filterSubject;
                return GetThis();
            }

            /// <summary>
            /// Set the external reference.
            /// </summary>
            /// <param name="external">the external</param>
            /// <returns>The Builder</returns>
            public TBuilder WithExternal(External external)
            {
                _external = external;
                return GetThis();
            }

            /// <summary>
            /// Set the external reference by using a domain based prefix.
            /// </summary>
            /// <param name="domain">the domain</param>
            /// <returns>The Builder</returns>
            public TBuilder WithDomain(string domain)
            {
                string prefix = JetStreamOptions.ConvertDomainToPrefix(domain);
                _external = prefix == null ? null : External.Builder().WithApi(prefix).Build();
                return GetThis();
            }

            /// <summary>
            /// Set the subject transforms.
            /// </summary>
            /// <param name="subjectTransforms">the subjectTransforms</param>
            /// <returns>The Builder</returns>
            public TBuilder WithSubjectTransforms(params SubjectTransform[] subjectTransforms)
            {
                _subjectTransforms = subjectTransforms;
                return GetThis();
            }

            /// <summary>
            /// Set the subject transforms.
            /// </summary>
            /// <param name="subjectTransforms">the subjectTransforms</param>
            /// <returns>The Builder</returns>
            public TBuilder WithSubjectTransforms(IList<SubjectTransform> subjectTransforms)
            {
                _subjectTransforms = subjectTransforms;
                return GetThis();
            }

            /// <summary>
            /// Build a Source object
            /// </summary>
            /// <returns>The Source</returns>
            public abstract TSourceBase Build();
        }

        protected bool Equals(SourceBase other)
        {
            return Name == other.Name 
                   && StartSeq == other.StartSeq 
                   && StartTime.Equals(other.StartTime) 
                   && FilterSubject == other.FilterSubject 
                   && Equals(External, other.External)
                   && Validator.SequenceEqual(SubjectTransforms, other.SubjectTransforms);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SourceBase)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (SubjectTransforms != null ? SubjectTransforms.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ StartSeq.GetHashCode();
                hashCode = (hashCode * 397) ^ StartTime.GetHashCode();
                hashCode = (hashCode * 397) ^ (FilterSubject != null ? FilterSubject.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (External != null ? External.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}