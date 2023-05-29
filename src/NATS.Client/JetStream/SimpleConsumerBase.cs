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

using System.Threading.Tasks;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
    /// </summary>
    internal class SimpleConsumerBase : ISimpleConsumer
    {
        protected JetStreamPullSubscription sub;
        protected PullMessageManager pmm;
        protected readonly object subLock;
        protected Task drainTask;

        public SimpleConsumerBase()
        {
            subLock = new object();
        }

        protected void InitSub(JetStreamPullSubscription sub)
        {
            this.sub = sub;
            pmm = (PullMessageManager)sub.MessageManager;
        }
        
        public ConsumerInfo GetConsumerInformation()
        {
            lock (subLock)
            {
                return sub.GetConsumerInformation();
            }
        }

        public Task Stop(int timeout)
        {
            lock (subLock)
            {
                if (drainTask == null)
                {
                    drainTask = sub.DrainAsync(timeout);
                }

                return drainTask;
            }
        }

        public void Dispose()
        {
            lock (subLock) {
                if (drainTask == null && sub.IsValid) {
                    sub.Unsubscribe();
                }
            }
        }
    }
}
