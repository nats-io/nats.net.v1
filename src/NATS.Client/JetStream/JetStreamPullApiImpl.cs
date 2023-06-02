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

        internal virtual void UpdateConsumer(string consumer)
        {
            _consumer = consumer;
        }
        
        internal string PullInternal(PullRequestOptions pullRequestOptions, bool raiseStatusWarnings, ITrackPendingListener trackPendingListener) {
            string publishSubject = _js.PrependPrefix(string.Format(JetStreamConstants.JsapiConsumerMsgNext, _stream, _consumer));
            string pullSubject = _subject.Replace("*", _pullSubjectIdHolder.Increment().ToString());
            _mm.StartPullRequest(pullSubject, pullRequestOptions, raiseStatusWarnings, trackPendingListener);	
            _conn.Publish(publishSubject, pullSubject, pullRequestOptions.Serialize());
            return pullSubject;
        }
        
        internal void Pull(int batchSize)
        {
            PullInternal(PullRequestOptions.Builder(batchSize).Build(), false, null);
        }

        internal void Pull(PullRequestOptions pullRequestOptions) {
            PullInternal(pullRequestOptions, false, null);
        }

        internal void PullNoWait(int batchSize)
        {
            PullInternal(PullRequestOptions.Builder(batchSize).WithNoWait().Build(), false, null);
        }

        internal void PullNoWait(int batchSize, int expiresInMillis)
        {
            Validator.ValidateDurationGtZeroRequired(expiresInMillis, "NoWait Expires In");
            PullInternal(PullRequestOptions.Builder(batchSize).WithNoWait().WithExpiresIn(expiresInMillis).Build(), false, null);
        }

        internal void PullExpiresIn(int batchSize, int expiresInMillis)
        {
            Validator.ValidateDurationGtZeroRequired(expiresInMillis, "Expires In");
            PullInternal(PullRequestOptions.Builder(batchSize).WithExpiresIn(expiresInMillis).Build(), false, null);
        }
    }
}