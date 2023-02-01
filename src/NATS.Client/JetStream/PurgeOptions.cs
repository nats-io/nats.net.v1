using System;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class PurgeOptions : JsonSerializable
    {
        /// <summary>
        /// The subject to purge
        /// </summary>
        public string Subject { get; }

        /// <summary>
        /// The upper-bound sequence for messages to be deleted
        /// </summary>
        public ulong Sequence { get; }

        /// <summary>
        /// The max number of messages to keep
        /// </summary>
        public ulong Keep { get; }

        private PurgeOptions(string subject, ulong sequence, ulong keep)
        {
            Subject = subject;
            Sequence = sequence;
            Keep = keep;
        }

        public override JSONNode ToJsonNode()
        {
            return new JSONObject
            {
                [ApiConstants.Filter] = Subject,
                [ApiConstants.Seq] = Sequence,
                [ApiConstants.Keep] = Keep
            };
        }

        /// <summary>
        /// Gets the purge options builder.
        /// </summary>
        /// <returns>
        /// The builder
        /// </returns>
        public static PurgeOptionsBuilder Builder()
        {
            return new PurgeOptionsBuilder();
        }

        /// <summary>
        /// Gets the purge options with a subject.
        /// </summary>
        /// <returns>
        /// The purge options
        /// </returns>
        public static PurgeOptions WithSubject(string subject)
        {
            return new PurgeOptionsBuilder().WithSubject(subject).Build();
        }

        /// <summary>
        /// The PurgeOptionsBuilder builds PurgeOptions
        /// </summary>
        public sealed class PurgeOptionsBuilder
        {
            private string _subject;
            private ulong _sequence;
            private ulong _keep;
            
            /// <summary>
            /// Set the subject to filter the purge. Wildcards allowed.
            /// </summary>
            /// <param name="subject">The subject</param>
            /// <returns>The Builder</returns>
            public PurgeOptionsBuilder WithSubject(string subject)
            {
                _subject = subject;
                return this;
            }

            /// <summary>
            /// Set upper-bound sequence for messages to be deleted
            /// </summary>
            /// <param name="sequence">The upper-bound sequence.</param>
            /// <returns>The PurgeOptionsBuilder</returns>
            public PurgeOptionsBuilder WithSequence(ulong sequence)
            {
                _sequence = sequence;
                return this;
            }

            /// <summary>
            /// Set the max number of messages to keep.
            /// </summary>
            /// <param name="keep">The max number of messages to keep.</param>
            /// <returns>The PurgeOptionsBuilder</returns>
            public PurgeOptionsBuilder WithKeep(ulong keep)
            {
                _keep = keep;
                return this;
            }

            /// <summary>
            /// Builds the PurgeOptions
            /// </summary>
            /// <returns>The PurgeOptions object.</returns>
            public PurgeOptions Build() 
            {
                Validator.ValidateSubject(_subject, false);
                
                if (_sequence > 0 && _keep > 0) {
                    throw new ArgumentException("seq and keep are mutually exclusive.");
                }

                return new PurgeOptions(_subject, _sequence, _keep);
            }
        }
    }
}
