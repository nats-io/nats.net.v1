// Copyright 2015-2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;

namespace NATS.Client
{
    /// <summary>
    /// The exception that is thrown when there is a NATS error condition.  All
    /// NATS exception inherit from this class.
    /// </summary>
    public class NATSException : Exception
    {
        public NATSException() : base() { }
        public NATSException(string err) : base (err) {}
        public NATSException(string err, Exception innerEx) : base(err, innerEx) { }
    }

    /// <summary>
    /// The exception that is thrown when there is a connection error.
    /// </summary>
    public class NATSConnectionException : NATSException
    {
        public NATSConnectionException(string err) : base(err) { }
        public NATSConnectionException(string err, Exception innerEx) : base(err, innerEx) { }
    }

    internal static class NATSConnectionExceptionExtensions
    {
        internal static bool IsAuthorizationViolationError(this NATSConnectionException ex)
            => ex?.Message.Equals("'authorization violation'", StringComparison.OrdinalIgnoreCase) == true;
        
        internal static bool IsAuthenticationExpiredError(this NATSConnectionException ex)
            => ex?.Message.Equals("'authentication expired'", StringComparison.OrdinalIgnoreCase) == true;
    }

    /// <summary>
    /// The exception that is thrown when there is an error writing
    /// to the internal reconnect buffer.
    /// </summary>
    public class NATSReconnectBufferException : NATSConnectionException
    {
        public NATSReconnectBufferException(string err) : base(err) { }
    }

    /// <summary>
    /// This exception that is thrown when there is an internal error with
    /// the NATS protocol.
    /// </summary>
    public class NATSProtocolException : NATSException
    {
        public NATSProtocolException(string err) : base(err) { }
    }

    /// <summary>
    /// The exception that is thrown when a connection cannot be made
    /// to any server.
    /// </summary>
    public class NATSNoServersException : NATSException
    {
        public NATSNoServersException(string err) : base(err) { }
    }

    /// <summary>
    /// The exception that is thrown when a secure connection is requested,
    /// but not required.
    /// </summary>
    public class NATSSecureConnWantedException : NATSException
    {
        public NATSSecureConnWantedException() : base("A secure connection is requested.") { }
    }

    /// <summary>
    /// The exception that is thrown when a secure connection is required.
    /// </summary>
    public class NATSSecureConnRequiredException : NATSException
    {
        public NATSSecureConnRequiredException() : base("A secure connection is required.") { }
        public NATSSecureConnRequiredException(string s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a an operation is performed on
    /// a connection that is closed.
    /// </summary>
    public class NATSConnectionClosedException : NATSException
    {
        public NATSConnectionClosedException() : base("Connection is closed.") { }
    }

    /// <summary>
    /// The exception that is thrown when a consumer (subscription) is slow.
    /// </summary>
    public class NATSSlowConsumerException : NATSException
    {
        public NATSSlowConsumerException() : base("Consumer is too slow.") { }
    }

    /// <summary>
    /// The exception that is thrown when an operation occurs on a connection
    /// that has been determined to be stale.
    /// </summary>
    public class NATSStaleConnectionException : NATSException
    {
        public NATSStaleConnectionException() : base("Connection is stale.") { }
    }

    /// <summary>
    /// The exception that is thrown when a message payload exceeds what
    /// the maximum configured.
    /// </summary>
    public class NATSMaxPayloadException : NATSException
    {
        public NATSMaxPayloadException() : base("Maximum payload size has been exceeded") { }
        public NATSMaxPayloadException(string err) : base(err) { }
    }

    /// <summary>
    /// The exception that is thrown when a subscriber has exceeded the maximum
    /// number of messages that has been configured.
    /// </summary>
    public class NATSMaxMessagesException : NATSException
    {
        public NATSMaxMessagesException() : base("Maximum number of messages have been exceeded.") { }
    }
    
    /// <summary>
    /// The exception that is thrown when a subscriber operation is performed on
    /// an invalid subscriber.
    /// </summary>
    public class NATSBadSubscriptionException : NATSException
    {
        public NATSBadSubscriptionException() : base("Subscription is not valid.") { }
        public NATSBadSubscriptionException(string s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a NATS operation times out.
    /// </summary>
    public class NATSTimeoutException : NATSException
    {
        public NATSTimeoutException() : base("Timeout occurred.") { }
        public NATSTimeoutException(string s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a NATS operation is not supported due
    /// to client and server feature conflict.
    /// </summary>
    public class NATSNotSupportedException : NATSException
    {
        public NATSNotSupportedException() : base("Operation not supported.") { }
        public NATSNotSupportedException(string s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a NATS header is invalid.
    /// </summary>
    public class NATSInvalidHeaderException : NATSException
    {
        public NATSInvalidHeaderException() : base("Invalid message header.") { }
        public NATSInvalidHeaderException(string s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a NATS operation is performed on a draining connection.
    /// </summary>
    public class NATSConnectionDrainingException : NATSConnectionException
    {
        public NATSConnectionDrainingException() : base("Connection is draining.") { }
    }

    /// <summary>
    /// The exception thrown when the server has detected there are no responders for a request.
    /// </summary>
    /// <remarks>
    /// This is circuit breaking behavior from the NATS server to more quickly identify when
    /// a request would have timed out.
    /// </remarks>
    public class NATSNoRespondersException : NATSTimeoutException
    {
        public NATSNoRespondersException() : base("No responders are available for the request.") { }
    }

    /// <summary>
    /// The exception that is thrown when a JetStream subscription detects an exceptional or unknown status
    /// </summary>
    public class NATSJetStreamStatusException : NATSException
    {
        public Subscription Sub { get; }
        public MsgStatus Status { get; }

        public NATSJetStreamStatusException(MsgStatus status, Subscription sub = null)
            : base($"{(status == null ? "Unknown or unprocessed status message" : status.Message)}")
        {
            Sub = sub;
            Status = status;
        }
    }

    /// <summary>
    /// The exception that is thrown when a client request is improper
    /// </summary>
    public class NATSJetStreamClientException : NATSException
    {
        private ClientExDetail _detail;

        internal NATSJetStreamClientException(ClientExDetail detail) : base(detail.Message)
        {
            _detail = detail;
        }

        public string Id => _detail.Id;
    }

    public sealed class ClientExDetail
    {
        public static readonly ClientExDetail JsSubPullCantHaveDeliverGroup = new ClientExDetail(Sub, 90001, "Pull subscriptions can't have a deliver group.");
        public static readonly ClientExDetail JsSubPullCantHaveDeliverSubject = new ClientExDetail(Sub, 90002, "Pull subscriptions can't have a deliver subject.");
        public static readonly ClientExDetail JsSubPushCantHaveMaxPullWaiting = new ClientExDetail(Sub, 90003, "Push subscriptions cannot supply max pull waiting.");
        public static readonly ClientExDetail JsSubQueueDeliverGroupMismatch = new ClientExDetail(Sub, 90004, "Queue / deliver group mismatch.");
        public static readonly ClientExDetail JsSubFcHbNotValidPull = new ClientExDetail(Sub, 90005, "Flow Control and/or heartbeat is not valid with a pull subscription.");
        public static readonly ClientExDetail JsSubFcHbNotValidQueue = new ClientExDetail(Sub, 90006, "Flow Control and/or heartbeat is not valid in queue mode.");
        public static readonly ClientExDetail JsSubNoMatchingStreamForSubject = new ClientExDetail(Sub, 90007, "No matching streams for subject.");
        public static readonly ClientExDetail JsSubConsumerAlreadyConfiguredAsPush = new ClientExDetail(Sub, 90008, "Consumer is already configured as a push consumer.");
        public static readonly ClientExDetail JsSubConsumerAlreadyConfiguredAsPull = new ClientExDetail(Sub, 90009, "Consumer is already configured as a pull consumer.");
        public static readonly ClientExDetail JsSubSubjectDoesNotMatchFilter = new ClientExDetail(Sub, 90011, "Subject does not match consumer configuration filter.");
        public static readonly ClientExDetail JsSubConsumerAlreadyBound = new ClientExDetail(Sub, 90012, "Consumer is already bound to a subscription.");
        public static readonly ClientExDetail JsSubExistingConsumerNotQueue = new ClientExDetail(Sub, 90013, "Existing consumer is not configured as a queue / deliver group.");
        public static readonly ClientExDetail JsSubExistingConsumerIsQueue = new ClientExDetail(Sub, 90014, "Existing consumer  is configured as a queue / deliver group.");
        public static readonly ClientExDetail JsSubExistingQueueDoesNotMatchRequestedQueue = new ClientExDetail(Sub, 90015, "Existing consumer deliver group does not match requested queue / deliver group.");
        public static readonly ClientExDetail JsSubExistingConsumerCannotBeModified = new ClientExDetail(Sub, 90016, "Existing consumer cannot be modified.");
        public static readonly ClientExDetail JsSubConsumerNotFoundRequiredInBind = new ClientExDetail(Sub, 90017, "Consumer not found, required in bind mode.");
        public static readonly ClientExDetail JsSubOrderedNotAllowOnQueues = new ClientExDetail(Sub, 90018, "Ordered consumer not allowed on queues.");
        public static readonly ClientExDetail JsSubPushCantHaveMaxBatch = new ClientExDetail(Sub, 90019, "Push subscriptions cannot supply max batch.");
        public static readonly ClientExDetail JsSubPushCantHaveMaxBytes = new ClientExDetail(Sub, 90020, "Push subscriptions cannot supply max bytes.");
        public static readonly ClientExDetail JsSubSubjectNeededToLookupStream = new ClientExDetail(Sub, 90022, "Subject needed to lookup stream. Provide either a subscribe subject or a ConsumerConfiguration filter subject.");

        /* Not used in this client. */ // public static readonly ClientExDetail JsSubPushAsyncCantSetPending = new ClientExDetail(Sub, 90021, "Pending limits must be set directly on the dispatcher.");

        public static readonly ClientExDetail JsSoDurableMismatch = new ClientExDetail(So, 90101, "Builder durable must match the consumer configuration durable if both are provided.");
        public static readonly ClientExDetail JsSoDeliverGroupMismatch = new ClientExDetail(So, 90102, "Builder deliver group must match the consumer configuration deliver group if both are provided.");
        public static readonly ClientExDetail JsSoDeliverSubjectMismatch = new ClientExDetail(So, 90103, "Builder deliver subject must match the consumer configuration deliver subject if both are provided.");
        public static readonly ClientExDetail JsSoOrderedNotAllowedWithBind = new ClientExDetail(So, 90104, "Bind is not allowed with an ordered consumer.");
        public static readonly ClientExDetail JsSoOrderedNotAllowedWithDeliverGroup = new ClientExDetail(So, 90105, "Deliver group is not allowed with an ordered consumer.");
        public static readonly ClientExDetail JsSoOrderedNotAllowedWithDurable = new ClientExDetail(So, 90106, "Durable is not allowed with an ordered consumer.");
        public static readonly ClientExDetail JsSoOrderedNotAllowedWithDeliverSubject = new ClientExDetail(So, 90107, "Deliver subject is not allowed with an ordered consumer.");
        public static readonly ClientExDetail JsSoOrderedRequiresAckPolicyNone = new ClientExDetail(So, 90108, "Ordered consumer requires Ack Policy None.");
        public static readonly ClientExDetail JsSoOrderedRequiresMaxDeliverOfOne = new ClientExDetail(So, 90109, "Max deliver is limited to 1 with an ordered consumer.");
        public static readonly ClientExDetail JsSoNameMismatch = new ClientExDetail(So, 90110, "Builder name must match the consumer configuration name if both are provided.");
        public static readonly ClientExDetail JsSoOrderedMemStorageNotSuppliedOrTrue = new ClientExDetail(So, 90111, "Mem Storage must be true if supplied.");
        public static readonly ClientExDetail JsSoOrderedReplicasNotSuppliedOrOne = new ClientExDetail(So, 90112, "Replicas must be 1 if supplied.");
        public static readonly ClientExDetail JsSoNameOrDurableRequiredForBind = new ClientExDetail(So, 90113, "Name or Durable required for Bind.");

        public static readonly ClientExDetail OsObjectNotFound = new ClientExDetail(Os, 90201, "The object was not found.");
        public static readonly ClientExDetail OsObjectIsDeleted = new ClientExDetail(Os, 90202, "The object is deleted.");
        public static readonly ClientExDetail OsObjectAlreadyExists = new ClientExDetail(Os, 90203, "An object with that name already exists.");
        public static readonly ClientExDetail OsCantLinkToLink = new ClientExDetail(Os, 90204, "A link cannot link to another link.");
        public static readonly ClientExDetail OsGetDigestMismatch = new ClientExDetail(Os, 90205, "Digest does not match meta data.");
        public static readonly ClientExDetail OsGetChunksMismatch = new ClientExDetail(Os, 90206, "Number of chunks does not match meta data.");
        public static readonly ClientExDetail OsGetSizeMismatch = new ClientExDetail(Os, 90207, "Total size does not match meta data.");
        public static readonly ClientExDetail OsGetLinkToBucket = new ClientExDetail(Os, 90208, "Cannot get object, it is a link to a bucket.");
        public static readonly ClientExDetail OsLinkNotAllowOnPut = new ClientExDetail(Os, 90209, "Link not allowed in metadata when putting an object.");

        public static readonly ClientExDetail JsConsumerCreate290NotAvailable = new ClientExDetail(Con, 90301, "Name field not valid when v2.9.0 consumer create api is not available.");
        public static readonly ClientExDetail JsConsumerNameDurableMismatch = new ClientExDetail(Con, 90302, "Name must match durable if both are supplied.");
        public static readonly ClientExDetail JsMultipleFilterSubjects210NotAvailable = new ClientExDetail(Con, 90303, "Multiple filter subjects not available until server version 2.10.0.");

        private const string Sub = "SUB";
        private const string So = "SO";
        private const string Os = "OS";
        private const string Con = "CON";

        public string Id { get; }
        public string Message { get; }

        internal ClientExDetail(string group, int code, string description)
        {
            Id = $"{group}-{code}";
            Message = $"[{Id}] {description}";
        }
        internal ClientExDetail(ClientExDetail ced, string extraMessage)
        {
            Id = ced.Id;
            Message = $"{ced.Message} {extraMessage}";
        }

        internal NATSJetStreamClientException Instance()
        {
            return new NATSJetStreamClientException(this);
        }

        internal NATSJetStreamClientException Instance(string extraMessage)
        {
            return new NATSJetStreamClientException(new ClientExDetail(this, extraMessage));
        }

        [Obsolete("constant name had typo, replaced with JsSubFcHbNotValidQueue")]
        public static readonly ClientExDetail JsSubFcHbHbNotValidQueue = new ClientExDetail(Sub, 90006, "Flow Control and/or heartbeat is not valid in queue mode.");
        
        [Obsolete("constant name had typo, replaced with JsSoDeliverSubjectMismatch")]
        public static readonly ClientExDetail JsSoDeliverSubjectGroupMismatch = new ClientExDetail(So, 90103, "Builder deliver subject must match the consumer configuration deliver subject if both are provided.");

        [Obsolete("replaced with more comprehensive name, replaced with JsConsumerCreate290NotAvailable")]
        public static readonly ClientExDetail JsConsumerCantUseNameBefore290 = new ClientExDetail(Con, 90301, "Name field not valid against pre v2.9.0 servers.");

        [Obsolete("replaced with more comprehensive name, replaced with JsSoOrderedRequiresMaxDeliverOfOne")]
        public static readonly ClientExDetail JsSoOrderedRequiresMaxDeliver = new ClientExDetail(So, 90109, "Max deliver is limited to 1 with an ordered consumer.");
    }
}
