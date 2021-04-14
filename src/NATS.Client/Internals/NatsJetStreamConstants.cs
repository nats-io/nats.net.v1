namespace NATS.Client.publics
{
    public sealed class NatsJetStreamConstants
    {
        public const string JS_PREFIX = "$JS.";
        
        public const string JSAPI_PREFIX = JS_PREFIX + "API.";

        // JSAPI_ACCOUNT_INFO is for obtaining general information about JetStream.
        public const string JSAPI_ACCOUNT_INFO = "INFO";

        // JSAPI_CONSUMER_CREATE is used to create consumers.
        public const string JSAPI_CONSUMER_CREATE = "CONSUMER.CREATE.%s";

        // JSAPI_DURABLE_CREATE is used to create durable consumers.
        public const string JSAPI_DURABLE_CREATE = "CONSUMER.DURABLE.CREATE.%s.%s";

        // JSAPI_CONSUMER_INFO is used to create consumers.
        public const string JSAPI_CONSUMER_INFO = "CONSUMER.INFO.%s.%s";

        // JSAPI_CONSUMER_MSG_NEXT is the prefix for the request next message(s) for a consumer in worker/pull mode.
        public const string JSAPI_CONSUMER_MSG_NEXT = "CONSUMER.MSG.NEXT.%s.%s";

        // JSAPI_CONSUMER_DELETE is used to delete consumers.
        public const string JSAPI_CONSUMER_DELETE = "CONSUMER.DELETE.%s.%s";

        // JSAPI_CONSUMER_NAMES is used to return a list of consumer names
        public const string JSAPI_CONSUMER_NAMES = "CONSUMER.NAMES.%s";

        // JSAPI_CONSUMER_LIST is used to return all detailed consumer information
        public const string JSAPI_CONSUMER_LIST = "CONSUMER.LIST.%s";

        // JSAPI_STREAMS can lookup a stream by subject.
        public const string JSAPI_STREAMS = "STREAM.NAMES";

        // JSAPI_STREAM_CREATE is the endpoint to create new streams.
        public const string JSAPI_STREAM_CREATE = "STREAM.CREATE.%s";

        // JSAPI_STREAM_INFO is the endpoint to get information on a stream.
        public const string JSAPI_STREAM_INFO = "STREAM.INFO.%s";

        // JSAPI_STREAM_UPDATE is the endpoint to update existing streams.
        public const string JSAPI_STREAM_UPDATE = "STREAM.UPDATE.%s";

        // JSAPI_STREAM_DELETE is the endpoint to delete streams.
        public const string JSAPI_STREAM_DELETE = "STREAM.DELETE.%s";

        // JSAPI_STREAM_PURGE is the endpoint to purge streams.
        public const string JSAPI_STREAM_PURGE = "STREAM.PURGE.%s";

        // JSAPI_STREAM_NAMES is the endpoint that will return a list of stream names
        public const string JSAPI_STREAM_NAMES = "STREAM.NAMES";

        // JSAPI_STREAM_LIST is the endpoint that will return all detailed stream information
        public const string JSAPI_STREAM_LIST = "STREAM.LIST";

        // JSAPI_MSG_GET is the endpoint to get a message.
        public const string JSAPI_MSG_GET = "STREAM.MSG.GET.%s";

        // JSAPI_MSG_DELETE is the endpoint to remove a message.
        public const string JSAPI_MSG_DELETE = "STREAM.MSG.DELETE.%s";

        public const string MSG_ID_HDR = "Nats-Msg-Id";
        public const string EXPECTED_STREAM_HDR = "Nats-Expected-Stream";
        public const string EXPECTED_LAST_SEQ_HDR = "Nats-Expected-Last-Sequence";
        public const string EXPECTED_LAST_MSG_ID_HDR = "Nats-Expected-Last-Msg-Id";
    }
}