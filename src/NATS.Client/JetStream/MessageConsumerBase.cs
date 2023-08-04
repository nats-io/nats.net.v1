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

using System;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
    /// </summary>
    internal class MessageConsumerBase : IMessageConsumer
    {
        protected IJetStreamSubscription sub;
        protected PullMessageManager pmm;
        protected JetStreamPullApiImpl pullImpl;
        protected ConsumerInfo cachedConsumerInfo;

        public bool Stopped { get; protected set; }
        public bool Finished { get; protected set; }

        internal MessageConsumerBase(ConsumerInfo cachedConsumerInfo, IJetStreamSubscription inSub)
        {
            this.cachedConsumerInfo = cachedConsumerInfo;
            sub = inSub;
            if (sub is JetStreamPullSubscription syncSub)
            {
                pmm = (PullMessageManager)syncSub.MessageManager;
                pullImpl = syncSub.pullImpl;
            }
            else if (sub is JetStreamPullAsyncSubscription asyncSub)
            {
                pmm = (PullMessageManager)asyncSub.MessageManager;
                pullImpl = asyncSub.pullImpl;
            }
        }
        
        internal bool NoMorePending() {
            return pmm.pendingMessages < 1 || (pmm.trackingBytes && pmm.pendingBytes < 1);
        }

        public ConsumerInfo GetConsumerInformation()
        {
            // don't look up consumer info if it was never set - this check is for ordered consumer
            if (cachedConsumerInfo != null)
            {
                cachedConsumerInfo = sub.GetConsumerInformation();
            }
            return cachedConsumerInfo;
        }

        public ConsumerInfo GetCachedConsumerInformation()
        {
            return cachedConsumerInfo;
        }

        public void Stop(int timeout)
        {
            if (!Stopped)
            {
                try
                {
                    sub.DrainAsync(timeout);
                }
                finally
                {
                    Stopped = true;
                    Finished = true;
                }
            }
        }

        public void Dispose()
        {
            try
            {
                if (!Stopped && sub.IsValid)
                {
                    Stopped = true;
                    sub.Unsubscribe();
                }
            }
            catch (Exception)
            {
                // ignored
            }
            finally
            {
                Stopped = true;
                Finished = true;
            }
        }
    }
}
