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
    /// Interface definition for a Message Consumer
    /// </summary>
    internal class MessageConsumer : MessageConsumerBase, IPullManagerObserver
    {
        private readonly BaseConsumeOptions opts;
        private readonly int thresholdMessages;
        private readonly long thresholdBytes;
        private readonly SimplifiedSubscriptionMaker subscriptionMaker;
        private readonly EventHandler<MsgHandlerEventArgs> userMessageHandler;

        internal MessageConsumer(SimplifiedSubscriptionMaker subscriptionMaker,
            ConsumerInfo cachedConsumerInfo,
            BaseConsumeOptions opts,
            EventHandler<MsgHandlerEventArgs> userMessageHandler) 
            : base(cachedConsumerInfo)
        {
            this.subscriptionMaker = subscriptionMaker;
            this.opts = opts;
            this.userMessageHandler = userMessageHandler;

            int bm = opts.Messages;
            long bb = opts.Bytes;
            int rePullMessages = Math.Max(1, bm * opts.ThresholdPercent / 100);
            long rePullBytes = bb == 0 ? 0 : Math.Max(1, bb * opts.ThresholdPercent / 100);
            thresholdMessages = bm - rePullMessages;
            thresholdBytes = bb == 0 ? int.MinValue : bb - rePullBytes;

            DoSub();
        }

        public void HeartbeatError() {
            try {
                // just close the current sub and make another one.
                // this could go on endlessly
                Dispose();
                DoSub();
            }
            catch (Exception) {
                SetupHbAlarmToTrigger();
            }
        }

        void DoSub()
        {
            EventHandler<MsgHandlerEventArgs> mh = null;
            if (userMessageHandler != null)
            {
                mh = (sender, args) =>
                {
                    userMessageHandler.Invoke(sender, args);
                    if (Stopped && pmm.NoMorePending())
                    {
                        Finished = true;
                    }
                };
            }

            try
            {
                base.InitSub(subscriptionMaker.Subscribe(mh, pmm, null));
                Repull();
                Stopped = false;
                Finished = false;
            }
            catch (Exception)
            {
                SetupHbAlarmToTrigger();
            }
        }

        private void SetupHbAlarmToTrigger() {
            pmm.ResetTracking();
            pmm.InitOrResetHeartbeatTimer();
        }

        public void PendingUpdated()
        {
            if (!Stopped && (pmm.pendingMessages <= thresholdMessages || (pmm.trackingBytes && pmm.pendingBytes <= thresholdBytes)))
            {
                Repull();
            }
        }

        private void Repull() {
            int rePullMessages = Math.Max(1, opts.Messages - pmm.pendingMessages);
            long rePullBytes = opts.Bytes == 0 ? 0 : opts.Bytes - pmm.pendingBytes;
            PullRequestOptions pro = PullRequestOptions.Builder(rePullMessages)
                .WithMaxBytes(rePullBytes)
                .WithExpiresIn(opts.ExpiresInMillis)
                .WithIdleHeartbeat(opts.IdleHeartbeat)
                .Build();
            pullImpl.Pull(pro, false, this);
        }
    }
}
