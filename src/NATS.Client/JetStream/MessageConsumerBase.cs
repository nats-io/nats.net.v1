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
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Base class for Message Consumer implementations
    /// </summary>
    internal class MessageConsumerBase : IMessageConsumer
    {
        internal IJetStreamSubscription sub;
        internal PullMessageManager pmm;
        internal JetStreamPullApiImpl pullImpl;
        internal ConsumerInfo cachedConsumerInfo;
        internal string consumerName;

        private readonly InterlockedBoolean _stopped;
        private readonly InterlockedBoolean _finished;

        public bool Stopped
        {
            get
            {
                return _stopped.IsTrue();
            }
            
            internal set
            {
                _stopped.Set(value);
            }
        }

        public bool Finished
        {
            get
            {
                return _finished.IsTrue();
            }
            
            internal set
            {
                _finished.Set(value);
            }
        }
        
        internal MessageConsumerBase(ConsumerInfo cachedConsumerInfo)
        {
            this.cachedConsumerInfo = cachedConsumerInfo;
            if (cachedConsumerInfo != null) {
                this.consumerName = cachedConsumerInfo.Name;
            }
            _stopped = new InterlockedBoolean();
            _finished = new InterlockedBoolean();
        }

        internal void setConsumerName(string consumerName) {
            this.consumerName = consumerName;
        }

        internal void InitSub(IJetStreamSubscription inSub, bool clearCachedConsumerInfo)
        {
            sub = inSub;
            consumerName = sub.Consumer;
            if (clearCachedConsumerInfo)
            {
                cachedConsumerInfo = null;
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

        public string GetConsumerName()
        {
            if (consumerName == null && cachedConsumerInfo != null) {
                consumerName = cachedConsumerInfo.Name;
            }
            return consumerName;
        }

        public ConsumerInfo GetConsumerInformation()
        {
            if (cachedConsumerInfo == null)
            {
                cachedConsumerInfo = sub.GetConsumerInformation();
                consumerName = cachedConsumerInfo.Name;
            }
            return cachedConsumerInfo;
        }

        public ConsumerInfo GetCachedConsumerInformation()
        {
            return cachedConsumerInfo;
        }

        public void Stop()
        {
            Stopped = true;
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
        }
    }
}
