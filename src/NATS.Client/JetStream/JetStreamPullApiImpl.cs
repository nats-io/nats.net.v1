using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    internal class JetStreamPullApiImpl
    {
        private readonly InterlockedLong _pullSubjectIdHolder;
        private readonly Connection _conn;
        private readonly JetStream _js;
        private readonly MessageManager _mm;
        private readonly string _stream;
        private readonly string _subject;
        private string _consumer;

        internal JetStreamPullApiImpl(Connection conn, JetStream js, MessageManager messageManager, 
            string stream, string subject, string consumer)
        {
            _pullSubjectIdHolder = new InterlockedLong();
            _conn = conn;
            _js = js;
            _mm = messageManager;
            _stream = stream;
            _subject = subject;
            _consumer = consumer;
        }

        internal void UpdateConsumer(string consumer)
        {
            _consumer = consumer;
        }
        
        internal string Pull(bool raiseStatusWarnings, ITrackPendingListener trackPendingListener,
            PullRequestOptions pullRequestOptions) {
            string publishSubject = _js.PrependPrefix(string.Format(JetStreamConstants.JsapiConsumerMsgNext, _stream, _consumer));
            string pullSubject = _subject.Replace("*", _pullSubjectIdHolder.Increment().ToString());
            _mm.StartPullRequest(pullSubject, pullRequestOptions, raiseStatusWarnings, trackPendingListener);	
            _conn.Publish(publishSubject, pullSubject, pullRequestOptions.Serialize());
            return pullSubject;
        }
    }
}