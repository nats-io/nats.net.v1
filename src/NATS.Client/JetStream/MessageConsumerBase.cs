﻿// Copyright 2023 The NATS Authors
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
        protected bool stopped;
        protected ConsumerInfo lastConsumerInfo;

        internal MessageConsumerBase(ConsumerInfo lastConsumerInfo)
        {
            this.lastConsumerInfo = lastConsumerInfo;
        }

        protected void InitSub(IJetStreamSubscription inSub)
        {
            sub = inSub;
            if (lastConsumerInfo == null)
            {
                lastConsumerInfo = sub.GetConsumerInformation();
            }
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
            lastConsumerInfo = sub.GetConsumerInformation();
            return lastConsumerInfo;
        }

        public ConsumerInfo GetCachedConsumerInformation()
        {
            return lastConsumerInfo;
        }

        public void Stop(int timeout)
        {
            if (!stopped)
            {
                stopped = true;
                sub.DrainAsync(timeout);
            }
        }

        public void Dispose()
        {
            try
            {
                if (!stopped && sub.IsValid)
                {
                    stopped = true;
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
