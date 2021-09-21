namespace NATS.Client.Internals
{
    public static class JetStreamConstants
    {
        /// <summary>
        /// Maximum Pull Size for Pull subscriptions.
        /// </summary>
        public const int MaxPullSize = 256;

        /// <summary>
        /// The standard JetStream Prefix prefix 
        /// </summary>
        public const string PrefixDollarJsDot = "$JS.";

        /// <summary>
        /// The standard JetStream Prefix suffix 
        /// </summary>
        public const string PrefixApiDot = "API.";

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
        /// JetStream expected last sequence header name.
        /// </summary>
        public const string ExpLastSubjectSeqHeader = "Nats-Expected-Last-Subject-Sequence";

        /// <summary>
        /// JetStream expected last message ID header Name.
        /// </summary>
        public const string ExpLastIdHeader = "Nats-Expected-Last-Msg-Id";

        public const int JsConsumerNotFoundErr = 10014;
    }
}