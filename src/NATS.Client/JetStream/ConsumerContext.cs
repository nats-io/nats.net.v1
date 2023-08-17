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
    internal interface SimplifiedSubscriptionMaker {
        IJetStreamSubscription Subscribe(EventHandler<MsgHandlerEventArgs> handler = null);
    }

    internal class OrderedPullSubscribeOptionsBuilder : PullSubscribeOptions.PullSubscribeOptionsSubscribeOptionsBuilder
    {
        internal OrderedPullSubscribeOptionsBuilder(string streamName, ConsumerConfiguration cc)
        {
            WithStream(streamName);
            WithConfiguration(cc);
            _ordered = true;
        }
    }

    /// <summary>
    /// Implementation of IConsumerContext
    /// </summary>
    internal class ConsumerContext : IConsumerContext, SimplifiedSubscriptionMaker
    {
        private readonly object stateLock;
        private readonly StreamContext streamCtx;
        private readonly bool ordered;
        private readonly ConsumerConfiguration originalOrderedCc;
        private readonly string subscribeSubject;
        private readonly PullSubscribeOptions unorderedBindPso;
        
        private ConsumerInfo cachedConsumerInfo;
        private MessageConsumerBase lastConsumer;
        private ulong highestSeq;

        public string ConsumerName { get; }
        
        internal ConsumerContext(StreamContext sc, ConsumerInfo ci)
        {
            stateLock = new object();
            streamCtx = sc;
            ordered = false;
            ConsumerName = ci.Name;
            unorderedBindPso = PullSubscribeOptions.BindTo(streamCtx.StreamName, ci.Name);
            cachedConsumerInfo = ci;
        }
        
        internal ConsumerContext(StreamContext sc, OrderedConsumerConfiguration config)
        {
            stateLock = new object();
            streamCtx = sc;
            ordered = true;
            originalOrderedCc = ConsumerConfiguration.Builder()
                .WithFilterSubject(config.FilterSubject)
                .WithDeliverPolicy(config.DeliverPolicy)
                .WithStartSequence(config.StartSequence)
                .WithStartTime(config.StartTime)
                .WithReplayPolicy(config.ReplayPolicy)
                .WithHeadersOnly(config.HeadersOnly)
                .Build();
            subscribeSubject = originalOrderedCc.FilterSubject;
            unorderedBindPso = null;

        }
        
        public IJetStreamSubscription Subscribe(EventHandler<MsgHandlerEventArgs> handler = null) {
            PullSubscribeOptions pso;
            if (ordered) {
                if (lastConsumer != null) {
                    highestSeq = Math.Max(highestSeq, lastConsumer.pmm.LastStreamSeq);
                }
                ConsumerConfiguration cc = lastConsumer == null
                    ? originalOrderedCc
                    : streamCtx.js.NextOrderedConsumerConfiguration(originalOrderedCc, highestSeq, null);
                pso = new OrderedPullSubscribeOptionsBuilder(streamCtx.StreamName, cc).Build();
            }
            else {
                pso = unorderedBindPso;
            }

            if (handler == null) {
                return (JetStreamPullSubscription)streamCtx.js.PullSubscribe(subscribeSubject, pso);
            }
            return (JetStreamPullAsyncSubscription)streamCtx.js.PullSubscribeAsync(subscribeSubject, handler, pso);
        }
    
        private void CheckState() {
            if (lastConsumer != null) {
                if (ordered) {
                    if (!lastConsumer.Finished) {
                        throw new InvalidOperationException("The ordered consumer is already receiving messages. Ordered Consumer does not allow multiple instances at time.");
                    }
                }
                if (lastConsumer.Finished && !lastConsumer.Stopped) {
                    lastConsumer.Dispose(); // finished, might as well make sure the sub is closed.
                }
            }
        }

        private MessageConsumerBase TrackConsume(MessageConsumerBase con) {
            lastConsumer = con;
            return con;
        }

        public ConsumerInfo GetConsumerInfo()
        {
            cachedConsumerInfo = streamCtx.jsm.GetConsumerInfo(streamCtx.StreamName, cachedConsumerInfo.Name);
            return cachedConsumerInfo;
        }

        public ConsumerInfo GetCachedConsumerInfo()
        {
            return cachedConsumerInfo;
        }

        public Msg Next(int maxWaitMillis = DefaultExpiresInMillis)
        {
            lock (stateLock)
            {
                CheckState();
                if (maxWaitMillis < MinExpiresMills) 
                {
                    throw new ArgumentException($"Max wait must be at least {MinExpiresMills} milliseconds.");
                }
            }

            using (MessageConsumerBase con = new MessageConsumerBase(cachedConsumerInfo))
            {
                try
                {
                    JetStreamPullSubscription sub = (JetStreamPullSubscription)Subscribe();
                    con.InitSub(sub);
                    con.pullImpl.Pull(PullRequestOptions.Builder(1)
                        .WithExpiresIn(maxWaitMillis - JetStreamPullSubscription.ExpireAdjustment)
                        .Build(), false, null);
                    TrackConsume(con);
                    Msg m = sub.NextMessage(maxWaitMillis);
                    con.Finished = true;
                    return m;
                }
                catch (NATSTimeoutException)
                {
                    return null;
                }
                finally
                {
                    con.Finished = true;
                }
            }
        }

        public IFetchConsumer FetchMessages(int maxMessages) {
            return Fetch(FetchConsumeOptions.Builder().WithMaxMessages(maxMessages).Build());
        }

        public IFetchConsumer FetchBytes(int maxBytes) {
            return Fetch(FetchConsumeOptions.Builder().WithMaxBytes(maxBytes).Build());
        }

        public IFetchConsumer Fetch(FetchConsumeOptions fetchConsumeOptions) {
            lock (stateLock)
            {
                CheckState();
                Validator.Required(fetchConsumeOptions, "Fetch Consume Options");
                return (IFetchConsumer)TrackConsume(new FetchConsumer(this, cachedConsumerInfo, fetchConsumeOptions));
            }
        }

        public IIterableConsumer Iterate(ConsumeOptions consumeOptions = null) {
            lock (stateLock)
            {
                CheckState();
                return (IIterableConsumer)TrackConsume(new IterableConsumer(this, consumeOptions ?? DefaultConsumeOptions, cachedConsumerInfo));
            }
        }

        public IMessageConsumer Consume(EventHandler<MsgHandlerEventArgs> handler, ConsumeOptions consumeOptions = null) {
            lock (stateLock)
            {
                CheckState();
                Validator.Required(handler, "Msg Handler");
                return TrackConsume(new MessageConsumer(this, consumeOptions ?? DefaultConsumeOptions, cachedConsumerInfo, handler));
            }
        }
    }
}
