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
    internal class MessageConsumer : MessageConsumerBase, ITrackPendingListener
    {
        private readonly PullRequestOptions rePullPro;
        private readonly int thresholdMessages;
        private readonly long thresholdBytes;

        internal MessageConsumer(SubscriptionMaker subscriptionMaker, EventHandler<MsgHandlerEventArgs> messageHandler, BaseConsumeOptions opts) {
            InitSub(subscriptionMaker.MakeSubscription(messageHandler));

            int bm = opts.Messages;
            long bb = opts.Bytes;

            int rePullMessages = Math.Max(1, bm * opts.ThresholdPercent / 100);
            long rePullBytes = bb == 0 ? 0 : Math.Max(1, bb * opts.ThresholdPercent / 100);
            rePullPro = PullRequestOptions.Builder(rePullMessages)
                .WithMaxBytes(rePullBytes)
                .WithExpiresIn(opts.ExpiresIn)
                .WithIdleHeartbeat(opts.IdleHeartbeat)
                .Build();

            thresholdMessages = bm - rePullMessages;
            thresholdBytes = bb == 0 ? int.MinValue : bb - rePullBytes;

            pullImpl.Pull(false, this, PullRequestOptions.Builder(bm)
                .WithMaxBytes(bb)
                .WithExpiresIn(opts.ExpiresIn)
                .WithIdleHeartbeat(opts.IdleHeartbeat)
                .Build());
        }

        public void Track(int pendingMessages, long pendingBytes, bool trackingBytes) {
            if (!stopped &&
                (pmm.pendingMessages <= thresholdMessages
                 || (pmm.trackingBytes && pmm.pendingBytes <= thresholdBytes)))
            {
                pullImpl.Pull(false, this, rePullPro);
            }
        }
    }
}
