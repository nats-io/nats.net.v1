// Copyright 2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Diagnostics;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Implementation of IFetchConsumer
    /// </summary>
    internal class FetchConsumer : MessageConsumerBase, IFetchConsumer
    {
        private readonly int maxWaitMillis;
        private readonly string pullSubject;
        private Stopwatch sw;

        internal FetchConsumer(SimplifiedSubscriptionMaker subscriptionMaker,
            ConsumerInfo cachedConsumerInfo,
            FetchConsumeOptions fetchConsumeOptions) 
            : base(cachedConsumerInfo)  
        {
            maxWaitMillis = fetchConsumeOptions.ExpiresInMillis;
            PullRequestOptions pro = PullRequestOptions.Builder(fetchConsumeOptions.MaxMessages)
                .WithMaxBytes(fetchConsumeOptions.MaxBytes)
                .WithExpiresIn(fetchConsumeOptions.ExpiresInMillis)
                .WithIdleHeartbeat(fetchConsumeOptions.IdleHeartbeat)
                .Build();
            InitSub(subscriptionMaker.Subscribe());
            pullSubject = ((JetStreamPullSubscription)sub).pullImpl.Pull(pro, false, null);
        }

        public Msg NextMessage()
        {
            bool timeoutSetFinished = false;

            try
            {
                if (Finished)
                {
                    return null;
                }
                
                // if the manager thinks it has received everything in the pull, it means
                // that all the messages are already in the internal queue and there is
                // no waiting necessary
                if (pmm.NoMorePending())
                {
                    timeoutSetFinished = true;
                    Msg m = ((JetStreamPullSubscription)sub)._nextUnmanagedNoWait(pullSubject);
                    if (m == null) {
                        // if there are no messages in the internal cache AND there are no more pending,
                        // they all have been read and we can go ahead and close the subscription.
                        Finished = true;
                        Dispose();
                    }
                    return m;
                }

                // by not starting the timer until the first call, it gives a little buffer around
                // the next message to account for latency of incoming messages
                int timeLeftMillis;
                if (sw == null)
                {
                    sw = Stopwatch.StartNew();
                    timeLeftMillis = maxWaitMillis;
                }
                else
                {
                    timeLeftMillis = maxWaitMillis - (int)sw.ElapsedMilliseconds;
                }

                // if the timer has run out, don't allow waiting
                // this might happen once, but it should already be noMorePending
                if (timeLeftMillis < 1)
                {
                    return ((JetStreamPullSubscription)sub)._nextUnmanagedNoWait(pullSubject); // 1 is the shortest time I can give
                }

                return ((JetStreamPullSubscription)sub)._nextUnmanaged(timeLeftMillis, pullSubject);
            }
            catch (NATSTimeoutException)
            {
                if (timeoutSetFinished)
                {
                    Finished = true;
                }
                return null;
            }
        }
    }
}
