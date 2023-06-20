// Copyright 2023 The NATS Authors
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
            pullImpl.Pull(PullRequestOptions.Builder(batchSize).Build(), true, null);
        }

        public void Pull(PullRequestOptions pullRequestOptions) {
            pullImpl.Pull(pullRequestOptions, true, null);
        }

        public void PullExpiresIn(int batchSize, int expiresInMillis)
        {
            Validator.ValidateDurationGtZeroRequired(expiresInMillis, "Expires In");
            pullImpl.Pull(PullRequestOptions.Builder(batchSize).WithExpiresIn(expiresInMillis).Build(), true, null);
        }

        public void PullNoWait(int batchSize)
        {
            pullImpl.Pull(PullRequestOptions.Builder(batchSize).WithNoWait().Build(), true, null);
        }

        public void PullNoWait(int batchSize, int expiresInMillis)
        {
            Validator.ValidateDurationGtZeroRequired(expiresInMillis, "NoWait Expires In");
            pullImpl.Pull(PullRequestOptions.Builder(batchSize).WithNoWait().WithExpiresIn(expiresInMillis).Build(), true, null);
        }
    }
}
