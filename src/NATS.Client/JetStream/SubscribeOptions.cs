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
using static NATS.Client.ClientExDetail;
using static NATS.Client.Internals.Validator;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// The base class for all Subscribe Options containing a stream and
    /// consumer configuration.
    /// </summary>
    public abstract class SubscribeOptions
    {
        public const long DefaultOrderedHeartbeat = 5000;

        public string Stream { get; }
        public bool Pull { get; }
        public bool Bind { get; }
        public bool Ordered { get; }
        internal int MessageAlarmTime { get; }
        public ConsumerConfiguration ConsumerConfiguration { get; }
        public long PendingMessageLimit { get; }
        public long PendingByteLimit { get; }
        
        /// <summary>
        /// Gets the durable name
        /// </summary>
        public string Durable => ConsumerConfiguration.Durable;

        /// <summary>
        /// Gets the deliver subject
        /// </summary>
        public string DeliverSubject => ConsumerConfiguration.DeliverSubject;

        /// <summary>
        /// Gets the deliver group
        /// </summary>
        public string DeliverGroup => ConsumerConfiguration.DeliverGroup;

        protected SubscribeOptions(ISubscribeOptionsBuilder builder, bool pull, bool ordered, 
            string deliverSubject, string deliverGroup,
            long pendingMessageLimit = Defaults.SubPendingMsgsLimit, 
            long pendingByteLimit = Defaults.SubPendingBytesLimit)
        {
            Pull = pull;
            Bind = builder.Bind;
            Ordered = ordered;
            MessageAlarmTime = builder.MessageAlarmTime;

            if (Ordered && Bind)
            {
                throw JsSoOrderedNotAllowedWithBind.Instance();
            }
            
            Stream = ValidateStreamName(builder.Stream, builder.Bind);
            
            string durable = ValidateMustMatchIfBothSupplied(builder.Durable, builder.Cc?.Durable, JsSoDurableMismatch);
            durable = ValidateDurable(durable, builder.Bind);

            string name = ValidateMustMatchIfBothSupplied(builder.Name, builder.Cc?.Name, JsSoNameMismatch);
            
            ValidateMustMatchIfBothSupplied(name, durable, JsConsumerNameDurableMismatch);

            deliverGroup = ValidateMustMatchIfBothSupplied(deliverGroup, builder.Cc?.DeliverGroup, JsSoDeliverGroupMismatch);

            deliverSubject = ValidateMustMatchIfBothSupplied(deliverSubject, builder.Cc?.DeliverSubject, JsSoDeliverSubjectMismatch);

            PendingMessageLimit = pendingMessageLimit;
            PendingByteLimit = pendingByteLimit;
            
            if (Ordered)
            {
                ValidateNotSupplied(deliverGroup, JsSoOrderedNotAllowedWithDeliverGroup);
                ValidateNotSupplied(durable, JsSoOrderedNotAllowedWithDurable);
                ValidateNotSupplied(deliverSubject, JsSoOrderedNotAllowedWithDeliverSubject);
                bool? ms = builder.Cc?._memStorage;
                if (ms != null && !ms.Value)
                {
                    throw JsSoOrderedMemStorageNotSuppliedOrTrue.Instance();
                }

                int? r = builder.Cc?._numReplicas;
                if (r != null && r != 1)
                {
                    throw JsSoOrderedReplicasNotSuppliedOrOne.Instance();
                }

                long hb = DefaultOrderedHeartbeat;

                if (builder.Cc != null)
                {
                    // want to make sure they didn't set it or they didn't set it to something other than none
                    if (builder.Cc._ackPolicy != null && builder.Cc._ackPolicy != AckPolicy.None) {
                        throw JsSoOrderedRequiresAckPolicyNone.Instance();
                    }
                    if (builder.Cc.MaxDeliver > 1) {
                        throw JsSoOrderedRequiresMaxDeliver.Instance();
                    }

                    Duration ccHb = builder.Cc.IdleHeartbeat;
                    if (ccHb != null)
                    {
                        hb = ccHb.Millis;
                    }
                }
                ConsumerConfiguration = ConsumerConfiguration.Builder(builder.Cc)
                    .WithAckPolicy(AckPolicy.None)
                    .WithMaxDeliver(1)
                    .WithFlowControl(hb)
                    .WithAckWait(Duration.OfHours(22))
                    .WithName(name)
                    .WithMemStorage(true)
                    .WithNumReplicas(1)
                    .Build();
            }
            else
            {
                ConsumerConfiguration = ConsumerConfiguration.Builder(builder.Cc)
                    .WithDurable(durable)
                    .WithDeliverSubject(deliverSubject)
                    .WithDeliverGroup(deliverGroup)
                    .WithName(name)
                    .Build();
            }
        }
        
        public interface ISubscribeOptionsBuilder
        {
            string Stream { get; }
            bool Bind { get; }
            string Durable { get; }
            string Name { get; }
            ConsumerConfiguration Cc { get; }
            int MessageAlarmTime { get; }
        }
            
        public abstract class SubscribeOptionsBuilder<TB, TSo> : ISubscribeOptionsBuilder
        {
            string _stream;
            bool _bind;
            string _durable;
            string _name;
            ConsumerConfiguration _config;
            int _messageAlarmTime = -1;

            public string Stream => _stream;
            public bool Bind => _bind;
            public string Durable => _durable;
            public string Name => _name;
            public ConsumerConfiguration Cc => _config;
            public int MessageAlarmTime => _messageAlarmTime;

            protected abstract TB GetThis();

            /// <summary>
            /// Set the stream name
            /// </summary>
            /// <param name="stream">the stream name</param>
            /// <returns>The builder</returns>
            public TB WithStream(string stream)
            {
                _stream = ValidateStreamName(stream, false);
                return GetThis();
            }

            /// <summary>
            /// Sets the durable name for the consumer.
            /// Null or empty clears the field
            /// </summary>
            /// <param name="durable">the durable value</param>
            /// <returns>The B</returns>
            public TB WithDurable(string durable)
            {
                _durable = ValidateDurable(durable, false);
                return GetThis();
            }

            /// <summary>
            /// Sets the name for the consumer.
            /// Null or empty clears the field
            /// </summary>
            /// <param name="name">the durable value</param>
            /// <returns>The B</returns>
            public TB WithName(string name)
            {
                _name = ValidateConsumerName(name, false);
                return GetThis();
            }

            /// <summary>
            /// Set as a direct subscribe
            /// </summary>
            /// <returns>The builder</returns>
            public TB WithBind(bool isBind)
            {
                _bind = isBind;
                return GetThis();
            }

            /// <summary>
            /// Set the ConsumerConfiguration
            /// </summary>
            /// <param name="configuration">the ConsumerConfiguration object</param>
            /// <returns>The builder</returns>
            public TB WithConfiguration(ConsumerConfiguration configuration)
            {
                _config = configuration;
                return GetThis();
            }

            /// <summary>
            /// Set the total amount of time to not receive any messages or heartbeats
            /// before calling the ErrorListener heartbeatAlarm 
            /// </summary>
            /// <param name="messageAlarmTime"> the time</param>
            /// <returns>The builder</returns>
            public TB WithMessageAlarmTime(int messageAlarmTime)
            {
                _messageAlarmTime = messageAlarmTime;
                return GetThis();
            }

            /// <summary>
            /// Builds the SubscribeOptions
            /// </summary>
            /// <returns>The SubscribeOptions object.</returns>
            public abstract TSo Build();
        }
    }
}
