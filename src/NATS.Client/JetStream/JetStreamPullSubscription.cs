// Copyright 2021 The NATS Authors
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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public class JetStreamPullSubscription : JetStreamAbstractSyncSubscription, IJetStreamPullSubscription
    {
        private readonly InterlockedLong pullSubjectIdHolder;

        internal JetStreamPullSubscription(Connection conn, string subject,
            JetStream js, string stream, string consumer, string deliver,
            MessageManager messageManager)
            : base(conn, subject, null, js, stream, consumer, deliver, messageManager)
        {
            pullSubjectIdHolder = new InterlockedLong();
        }

        public bool IsPullMode() => true;
        
        public void Pull(int batchSize)
        {
            _pullInternal(PullRequestOptions.Builder(batchSize).Build(), false, null);
        }

        public void Pull(PullRequestOptions pullRequestOptions) {
            _pullInternal(pullRequestOptions, false, null);
        }

        public string _pullInternal(PullRequestOptions pullRequestOptions, bool raiseStatusWarnings, ITrackPendingListener trackPendingListener) {
            string publishSubject = Context.PrependPrefix(string.Format(JetStreamConstants.JsapiConsumerMsgNext, Stream, Consumer));
            string pullSubject = Subject.Replace("*", pullSubjectIdHolder.Increment().ToString());
            Console.WriteLine("---> PLLSB " + pullSubject);
            MessageManager.StartPullRequest(pullSubject, pullRequestOptions, raiseStatusWarnings, trackPendingListener);	
            Connection.Publish(publishSubject, pullSubject, pullRequestOptions.Serialize());
            return pullSubject;
        }

        public void PullExpiresIn(int batchSize, int expiresInMillis)
        {
            DurationGtZeroRequired(expiresInMillis, "Expires In");
            _pullInternal(PullRequestOptions.Builder(batchSize).WithExpiresIn(expiresInMillis).Build(), false, null);
        }

        public void PullNoWait(int batchSize)
        {
            _pullInternal(PullRequestOptions.Builder(batchSize).WithNoWait().Build(), false, null);
        }

        public void PullNoWait(int batchSize, int expiresInMillis)
        {
            DurationGtZeroRequired(expiresInMillis, "NoWait Expires In");
            _pullInternal(PullRequestOptions.Builder(batchSize).WithNoWait().WithExpiresIn(expiresInMillis).Build(), false, null);
        }

        private void DurationGtZeroRequired(long millis, string label) {
            if (millis <= 0) {
                throw new ArgumentException(label + " wait duration must be supplied and greater than 0.");
            }
        }

        internal const int ExpireAdjustment = 10;
        internal const int MinExpireMillis = 20;

        public IList<Msg> Fetch(int batchSize, int maxWaitMillis)
        {
            DurationGtZeroRequired(maxWaitMillis, "Fetch");

            IList<Msg> messages = new List<Msg>();
            int batchLeft = batchSize;
            
            Stopwatch sw = Stopwatch.StartNew();

            Duration expires = Duration.OfMillis(
                maxWaitMillis > ExpireAdjustment ? maxWaitMillis - ExpireAdjustment : maxWaitMillis);
            string pullSubject = _pullInternal(PullRequestOptions.Builder(batchLeft).WithExpiresIn(expires).Build(), false, null);

            try
            {
                // timeout > 0 process as many messages we can in that time period
                // If we get a message that either manager handles, we try again, but
                // with a shorter timeout based on what we already used up
                int timeLeft = maxWaitMillis;
                while (batchLeft > 0 && timeLeft > 0) {
                    Msg msg = NextMessageImpl(timeLeft);
                    if (!MessageManager.Manage(msg)) {
                        messages.Add(msg);
                        batchLeft--;
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
