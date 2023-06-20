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
        private readonly object subLock;
        
        protected IJetStreamSubscription sub;
        protected PullMessageManager pmm;
        protected JetStreamPullApiImpl pullImpl;
        protected bool stopped;

        internal MessageConsumerBase()
        {
            subLock = new object();
        }

        protected void InitSub(IJetStreamSubscription inSub)
        {
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
        
        public ConsumerInfo GetConsumerInformation()
        {
            lock (subLock)
            {
                return sub.GetConsumerInformation();
            }
        }

        public void Stop(int timeout)
        {
            lock (subLock)
            {
                if (!stopped)
                {
                    stopped = true;
                    sub.DrainAsync(timeout);
                }
            }
        }

        ~MessageConsumerBase()
        {
            try
            {
                if (!stopped && sub.IsValid)
                {
                    sub.Unsubscribe();
                }
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }
}
