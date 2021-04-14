namespace NATS.Client.Internals
{
    public sealed class NatsJetStreamConstants
    {
        public const int MaxPullSize = 256;
        
        public const string JsPrefix = "$JS.";
        
        public const string JsapiPrefix = JsPrefix + "API.";

        // JSAPI_ACCOUNT_INFO is for obtaining general information about JetStream.
        public const string JsapiAccountInfo = "INFO";

        // JSAPI_CONSUMER_CREATE is used to create consumers.
        public const string JsapiConsumerCreate = "CONSUMER.CREATE.%s";

        // JSAPI_DURABLE_CREATE is used to create durable consumers.
        public const string JsapiDurableCreate = "CONSUMER.DURABLE.CREATE.%s.%s";

        // JSAPI_CONSUMER_INFO is used to create consumers.
        public const string JsapiConsumerInfo = "CONSUMER.INFO.%s.%s";

        // JSAPI_CONSUMER_MSG_NEXT is the prefix for the request next message(s) for a consumer in worker/pull mode.
        public const string JsapiConsumerMsgNext = "CONSUMER.MSG.NEXT.%s.%s";

        // JSAPI_CONSUMER_DELETE is used to delete consumers.
        public const string JsapiConsumerDelete = "CONSUMER.DELETE.%s.%s";

        // JSAPI_CONSUMER_NAMES is used to return a list of consumer names
        public const string JsapiConsumerNames = "CONSUMER.NAMES.%s";

        // JSAPI_CONSUMER_LIST is used to return all detailed consumer information
        public const string JsapiConsumerList = "CONSUMER.LIST.%s";

        // JSAPI_STREAMS can lookup a stream by subject.
        public const string JsapiStreams = "STREAM.NAMES";

        // JSAPI_STREAM_CREATE is the endpoint to create new streams.
        public const string JsapiStreamCreate = "STREAM.CREATE.%s";

        // JSAPI_STREAM_INFO is the endpoint to get information on a stream.
        public const string JsapiStreamInfo = "STREAM.INFO.%s";

        // JSAPI_STREAM_UPDATE is the endpoint to update existing streams.
        public const string JsapiStreamUpdate = "STREAM.UPDATE.%s";

        // JSAPI_STREAM_DELETE is the endpoint to delete streams.
        public const string JsapiStreamDelete = "STREAM.DELETE.%s";

        // JSAPI_STREAM_PURGE is the endpoint to purge streams.
        public const string JsapiStreamPurge = "STREAM.PURGE.%s";

        // JSAPI_STREAM_NAMES is the endpoint that will return a list of stream names
        public const string JsapiStreamNames = "STREAM.NAMES";

        // JSAPI_STREAM_LIST is the endpoint that will return all detailed stream information
        public const string JsapiStreamList = "STREAM.LIST";

        // JSAPI_MSG_GET is the endpoint to get a message.
        public const string JsapiMsgGet = "STREAM.MSG.GET.%s";

        // JSAPI_MSG_DELETE is the endpoint to remove a message.
        public const string JsapiMsgDelete = "STREAM.MSG.DELETE.%s";

        public const string MsgIdHdr = "Nats-Msg-Id";
        public const string ExpectedStreamHdr = "Nats-Expected-Stream";
        public const string ExpectedLastSeqHdr = "Nats-Expected-Last-Sequence";
        public const string ExpectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id";
    }
}