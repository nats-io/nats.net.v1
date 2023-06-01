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
using static NATS.Client.JetStream.BaseConsumeOptions;
using static NATS.Client.JetStream.ConsumeOptions;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
    /// </summary>
    internal class ConsumerContext : IConsumerContext
    {
        internal readonly StreamContext streamContext;
        internal readonly JetStream js;
        internal ConsumerInfo lastConsumerInfo;

        public string ConsumerName => lastConsumerInfo.Name;
        
        internal ConsumerContext(StreamContext streamContext, ConsumerInfo ci)
        {
            this.streamContext = streamContext;
            js = new JetStream(streamContext.jsm.Conn, streamContext.jsm.JetStreamOptions);
            lastConsumerInfo = ci;
        }

        public ConsumerInfo GetConsumerInfo()
        {
            lastConsumerInfo = streamContext.jsm.GetConsumerInfo(streamContext.StreamName, lastConsumerInfo.Name);
            return lastConsumerInfo;
        }

        public ConsumerInfo GetCachedConsumerInfo()
        {
            return lastConsumerInfo;
        }

        public Msg Next() {
            return Next(DefaultExpiresInMillis);
        }

        public Msg Next(int maxWaitMillis) {
            if (maxWaitMillis < MinExpiresMills) {
                throw new ArgumentException($"Max wait must be at least {MinExpiresMills} milliseconds.");
            }

            long expires = maxWaitMillis - JetStreamPullSubscription.ExpireAdjustment;

            JetStreamPullSubscription sub = new SubscriptionMaker(this).makeSubscription(null);
            sub._pullInternal(PullRequestOptions.Builder(1).WithExpiresIn(expires).Build(), false, null);
            return sub.NextMessage(maxWaitMillis);
        }

        public IFetchConsumer FetchMessages(int maxMessages) {
            return Fetch(FetchConsumeOptions.Builder().WithMaxMessages(maxMessages).Build());
        }

        public IFetchConsumer FetchBytes(int maxBytes) {
            return Fetch(FetchConsumeOptions.Builder().WithMaxBytes(maxBytes).Build());
        }

        public IFetchConsumer Fetch(FetchConsumeOptions fetchConsumeOptions) {
            Validator.Required(fetchConsumeOptions, "Fetch Consume Options");
            return new FetchConsumer(new SubscriptionMaker(this), fetchConsumeOptions);
        }

        public IIterableConsumer consume() {
            return new IterableConsumer(new SubscriptionMaker(this), DefaultConsumeOptions);
        }

        public IIterableConsumer consume(ConsumeOptions consumeOptions) {
            Validator.Required(consumeOptions, "Consume Options");
            return new IterableConsumer(new SubscriptionMaker(this), consumeOptions);
        }

        public IMessageConsumer consume(EventHandler<MsgHandlerEventArgs> handler) {
            Validator.Required(handler, "Msg Handler");
            return new MessageConsumer(new SubscriptionMaker(this), handler, DefaultConsumeOptions);
        }

        public IMessageConsumer consume(EventHandler<MsgHandlerEventArgs> handler, ConsumeOptions consumeOptions) {
            Validator.Required(handler, "Msg Handler");
            Validator.Required(consumeOptions, "Consume Options");
            return new MessageConsumer(new SubscriptionMaker(this), handler, consumeOptions);
        }
    }

    internal class SubscriptionMaker
    {
        private ConsumerContext ctx;

        public SubscriptionMaker(ConsumerContext ctx)
        {
            this.ctx = ctx;
        }

        public JetStreamPullSubscription makeSubscription(EventHandler<MsgHandlerEventArgs> handler) {
            PullSubscribeOptions pso = PullSubscribeOptions.BindTo(ctx.streamContext.StreamName, ctx.ConsumerName);
            if (handler == null) {
                return (JetStreamPullSubscription)ctx.js.PullSubscribe(null, pso);
            }
            // return (JetStreamPullSubscription)ctx.js.PullSubscribeAsync(null, handler, pso);
            return null;
        }
    }
}