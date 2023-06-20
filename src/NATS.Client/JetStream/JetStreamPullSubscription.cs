// Copyright 2021-2023 The NATS Authors
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

using System.Collections.Generic;
using System.Diagnostics;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public class JetStreamPullSubscription : JetStreamAbstractSyncSubscription, IJetStreamPullSubscription
    {
        internal readonly JetStreamPullApiImpl pullImpl;

        internal JetStreamPullSubscription(Connection conn, string subject,
            JetStream js, string stream, string consumer, string deliver,
            MessageManager messageManager)
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

        internal const int ExpireAdjustment = 10;
        internal const int MinExpireMillis = 20;

        public IList<Msg> Fetch(int batchSize, int maxWaitMillis)
        {
            Validator.ValidateDurationGtZeroRequired(maxWaitMillis, "Fetch");

            IList<Msg> messages = new List<Msg>();
            int batchLeft = batchSize;
            
            Stopwatch sw = Stopwatch.StartNew();

            Duration expires = Duration.OfMillis(
                maxWaitMillis > ExpireAdjustment ? maxWaitMillis - ExpireAdjustment : maxWaitMillis);
            string pullSubject = pullImpl.Pull(PullRequestOptions.Builder(batchLeft).WithExpiresIn(expires).Build(), false, null);

            try
            {
                // timeout > 0 process as many messages we can in that time period
                // If we get a message that either manager handles, we try again, but
                // with a shorter timeout based on what we already used up
                int timeLeft = maxWaitMillis;
                while (batchLeft > 0 && timeLeft > 0) {
                    Msg msg = NextMessageImpl(timeLeft);
                    if (msg == null)
                    {
                        return messages;
                    }
                    switch (MessageManager.Manage(msg))
                    {
                        case ManageResult.Message:
                            messages.Add(msg);
                            batchLeft--;
                            break;
                        case ManageResult.StatusTerminus:
                            // if the status applies return null, otherwise it's ignored, fall through
                            if (pullSubject.Equals(msg.Subject))
                            {
                                return messages;
                            }
                            break;
                        case ManageResult.StatusError:
                            // if the status applies throw exception, otherwise it's ignored, fall through
                            if (pullSubject.Equals(msg.Subject))
                            {
                                throw new NATSJetStreamStatusException(msg.Status, this);
                            }
                            break;
                    }
                    // case STATUS, try again while we have time
                    timeLeft = maxWaitMillis - (int)sw.ElapsedMilliseconds;
                }
            }
            catch (NATSTimeoutException)
            {
                // regular timeout, just end
            }
            return messages;
        }
    }
}
