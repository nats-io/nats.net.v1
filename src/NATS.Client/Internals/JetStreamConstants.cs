using System;

namespace NATS.Client.Internals
{
    public static class JetStreamConstants
    {
        /// <summary>
        /// Maximum Pull Size for Pull subscriptions.
        /// </summary>
        [Obsolete("This property is obsolete. We do not enforce a limit on pull batch size.", false)]
        public const int MaxPullSize = 256;

        /// <summary>
        /// The Max History Per Key KV key
        /// </summary>
        public const int MaxHistoryPerKey = 64;

        /// <summary>
        /// The standard JetStream Prefix prefix 
        /// </summary>
        public const string PrefixDollarJsDot = "$JS.";

        /// <summary>
        /// The standard JetStream Prefix suffix 
        /// </summary>
        public const string PrefixApiDot = "API.";

        /// <summary>
        /// The standard JetStream Prefix suffix without the dot at the end 
        /// </summary>
        public const string PrefixApi = "API";

        /// <summary>
        /// The standard JetStream Prefix
        /// </summary>
        public const string DefaultApiPrefix = "$JS.API.";
        
        /// <summary>
        /// The standard JetStream API Prefix
        /// </summary>
        public const string JsAckSubjectPrefix = "$JS.ACK.";

        /// <summary>
        /// JSAPI_ACCOUNT_INFO is for obtaining general information about JetStream.
        /// </summary>
        public const string JsapiAccountInfo = "INFO";

        /// <summary>
        /// JSAPI_CONSUMER_CREATE is used to create consumers.
        /// </summary>
        public const string JsapiConsumerCreate = "CONSUMER.CREATE.{0}";

        public const string JsapiConsumerCreateV290 = "CONSUMER.CREATE.{0}.{1}";

        public const string JsapiConsumerCreateV290WithFilter = "CONSUMER.CREATE.{0}.{1}.{2}";

        /// <summary>
        /// JSAPI_DURABLE_CREATE is used to create durable consumers.
        /// </summary>
        public const string JsapiDurableCreate = "CONSUMER.DURABLE.CREATE.{0}.{1}";

        /// <summary>
        /// JSAPI_CONSUMER_INFO is used to create consumers.
        /// </summary>
        public const string JsapiConsumerInfo = "CONSUMER.INFO.{0}.{1}";

        /// <summary>
        /// JSAPI_CONSUMER_MSG_NEXT is the prefix for the request next message(s) for a consumer in worker/pull mode.
        /// </summary>
        public const string JsapiConsumerMsgNext = "CONSUMER.MSG.NEXT.{0}.{1}";

        /// <summary>
        /// JSAPI_CONSUMER_DELETE is used to delete consumers.
        /// </summary>
        public const string JsapiConsumerDelete = "CONSUMER.DELETE.{0}.{1}";

        /// <summary>
        /// JSAPI_CONSUMER_NAMES is used to return a list of consumer names
        /// </summary>
        public const string JsapiConsumerNames = "CONSUMER.NAMES.{0}";

        /// <summary>
        /// JSAPI_CONSUMER_LIST is used to return all detailed consumer information
        /// </summary>
        public const string JsapiConsumerList = "CONSUMER.LIST.{0}";

        /// <summary>
        /// JSAPI_STREAMS can lookup a stream by subject.
        /// </summary>
        public const string JsapiStreamNames = "STREAM.NAMES";

        /// <summary>
        /// JSAPI_STREAM_CREATE is the endpoint to create new streams.
        /// </summary>
        public const string JsapiStreamCreate = "STREAM.CREATE.{0}";

        /// <summary>
        /// JSAPI_STREAM_INFO is the endpoint to get information on a stream.
        /// </summary>
        public const string JsapiStreamInfo = "STREAM.INFO.{0}";

        /// <summary>
        /// JSAPI_STREAM_UPDATE is the endpoint to update existing streams.
        /// </summary>
        public const string JsapiStreamUpdate = "STREAM.UPDATE.{0}";

        /// <summary>
        /// JSAPI_STREAM_DELETE is the endpoint to delete streams.
        /// </summary>
        public const string JsapiStreamDelete = "STREAM.DELETE.{0}";

        /// <summary>
        /// JSAPI_STREAM_PURGE is the endpoint to purge streams.
        /// </summary>
        public const string JsapiStreamPurge = "STREAM.PURGE.{0}";

        /// <summary>
        /// JSAPI_STREAM_LIST is the endpoint that will return all detailed stream information
        /// </summary>
        public const string JsapiStreamList = "STREAM.LIST";

        /// <summary>
        /// JSAPI_MSG_GET is the endpoint to get a message.
        /// </summary>
        public const string JsapiMsgGet = "STREAM.MSG.GET.{0}";

        /// <summary>
        /// JSAPI_DIRECT_GET is the endpoint to directly get a message.
        /// </summary>
        public const string JsapiDirectGet = "DIRECT.GET.{0}";

        // JSAPI_DIRECT_GET_LAST is the preferred endpoint to direct get a last by subject message
        public const string JsapiDirectGetLast = "DIRECT.GET.{0}.{1}";

        /// <summary>
        /// JSAPI_MSG_DELETE is the endpoint to remove a message.
        /// </summary>
        public const string JsapiMsgDelete = "STREAM.MSG.DELETE.{0}";

        /// <summary>
        /// JetStream expected message ID header name.
        /// </summary>
        public const string MsgIdHeader = "Nats-Msg-Id";
        
        /// <summary>
        /// JetStream expected stream header name.
        /// </summary>
        public const string ExpStreamHeader = "Nats-Expected-Stream";
        
        /// <summary>
        /// JetStream expected last sequence header name.
        /// </summary>
        public const string ExpLastSeqHeader = "Nats-Expected-Last-Sequence";

        /// <summary>
        /// JetStream expected last message ID header Name.
        /// </summary>
        public const string ExpLastIdHeader = "Nats-Expected-Last-Msg-Id";
        
        /// <summary>
        /// JetStream expected last sequence header name.
        /// </summary>
        public const string ExpLastSubjectSeqHeader = "Nats-Expected-Last-Subject-Sequence";
        
        public const string LastConsumerHeader = "Nats-Last-Consumer";
        public const string LastStreamHeader = "Nats-Last-Stream";
        public const string ConsumerStalledHeader = "Nats-Consumer-Stalled";
        public const string MsgSizeHeader = "Nats-Msg-Size";

        public const string RollupHeader = "Nats-Rollup";
        public const string RollupHeaderSubject = "sub";
        public const string RollupHeaderAll = "all";

        public const string NatsStream       = "Nats-Stream";
        public const string NatsSequence     = "Nats-Sequence";
        public const string NatsTimestamp    = "Nats-Time-Stamp";
        public const string NatsSubject      = "Nats-Subject";
        public const string NatsLastSequence = "Nats-Last-Sequence";
        
        public const string NatsPendingMessages = "Nats-Pending-Messages";
        public const string NatsPendingBytes    = "Nats-Pending-Bytes";

        public const int JsConsumerNotFoundErr = 10014;
        public const int JsNoMessageFoundErr = 10037;
        public const int JsWrongLastSequence = 10071;

        public const string BadRequest                 = "Bad Request"; // 400
        public const string NoMessages                 = "No Messages"; // 404
        public const string ConsumerDeleted            = "Consumer Deleted"; // 409
        public const string ConsumerIsPushBased        = "Consumer is push based"; // 409

        public const string MessageSizeExceedsMaxBytes = "Message Size Exceeds MaxBytes"; // 409
        public const string ExceededMaxWaiting         = "Exceeded MaxWaiting"; // 409
        public const string ExceededMaxRequestBatch    = "Exceeded MaxRequestBatch"; // 409
        public const string ExceededMaxRequestExpires  = "Exceeded MaxRequestExpires"; // 409
        public const string ExceededMaxRequestMaxBytes = "Exceeded MaxRequestMaxBytes"; // 409

        public const string BatchCompleted             = "Batch Completed"; // 409 informational
 
        [Obsolete("This property is obsolete. Use LastConsumerHeader instead.", false)]
        public const string LastConsumerHdr = "Nats-Last-Consumer";

        [Obsolete("This property is obsolete. Use LastStreamHeader instead.", false)]
        public const string LastStreamHdr = "Nats-Last-Stream";
        
        [Obsolete("This property is obsolete. Use ConsumerStalledHeader instead.", false)]
        public const string ConsumerStalledHdr = "Nats-Consumer-Stalled";
        
        [Obsolete("This property is obsolete. Use RollupHeader instead.", false)]
        public const string RollupHdr = "Nats-Rollup";
        
        [Obsolete("This property is obsolete. Use MsgSizeHeader instead.", false)]
        public const string MsgSizeHdr = "Nats-Msg-Size";
    }
}