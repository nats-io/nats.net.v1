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
    /// SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
    /// </summary>
    internal class FetchConsumer : SimpleConsumerBase, IFetchConsumer
    {
        private readonly int maxWaitMillis;
        private Stopwatch sw;

        internal FetchConsumer(SubscriptionMaker subscriptionMaker, FetchConsumeOptions opts) 
        {
            InitSub(subscriptionMaker.makeSubscription(null));
            maxWaitMillis = opts.ExpiresIn;
        }

        public Msg nextMessage()
        {
            int timeLeftMillis;
            if (sw == null) {
                sw = Stopwatch.StartNew();
                timeLeftMillis = maxWaitMillis;
            }
            else
            {
                timeLeftMillis = maxWaitMillis - (int)sw.ElapsedMilliseconds;
            }

            // if the manager thinks it has received everything in the pull, it means
            // that all the messages are already in the internal queue and there is
            // no waiting necessary
            if (timeLeftMillis < 1 | pmm.pendingMessages < 1 || (pmm.trackingBytes && pmm.pendingBytes < 1))
            {
                return sub.NextMessage(1); // 1 is the shortest time I can give
            }

            return sub.NextMessage(timeLeftMillis);
        }
    }
}