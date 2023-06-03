using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public class JetStreamPullAsyncSubscription : JetStreamAbstractAsyncSubscription, IJetStreamPullAsyncSubscription
    {
        internal readonly JetStreamPullApiImpl pullImpl;

        internal JetStreamPullAsyncSubscription(Connection conn, string subject,
            JetStream js, string stream, string consumer, string deliver, MessageManager messageManager)
            : base(conn, subject, null, js, stream, consumer, deliver, messageManager)
        {
            pullImpl = new JetStreamPullApiImpl(conn, js, messageManager, stream, subject, consumer);
        }

        internal override void UpdateConsumer(string consumer)
        {
            base.UpdateConsumer(consumer);
            pullImpl.UpdateConsumer(consumer);
        }
        
        public bool IsPullMode() => true;

        public void Pull(int batchSize)
        {
            pullImpl.Pull(true, null, PullRequestOptions.Builder(batchSize).Build());
        }

        public void Pull(PullRequestOptions pullRequestOptions) {
            pullImpl.Pull(true, null, pullRequestOptions);
        }

        public void PullExpiresIn(int batchSize, int expiresInMillis)
        {
            Validator.ValidateDurationGtZeroRequired(expiresInMillis, "Expires In");
            pullImpl.Pull(true, null, PullRequestOptions.Builder(batchSize).WithExpiresIn(expiresInMillis).Build());
        }

        public void PullNoWait(int batchSize)
        {
            pullImpl.Pull(true, null, PullRequestOptions.Builder(batchSize).WithNoWait().Build());
        }

        public void PullNoWait(int batchSize, int expiresInMillis)
        {
            Validator.ValidateDurationGtZeroRequired(expiresInMillis, "NoWait Expires In");
            pullImpl.Pull(true, null, PullRequestOptions.Builder(batchSize).WithNoWait().WithExpiresIn(expiresInMillis).Build());
        }
    }
}