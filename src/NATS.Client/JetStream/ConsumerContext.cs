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
        IJetStreamSubscription Subscribe(EventHandler<MsgHandlerEventArgs> handler, PullMessageManager optionalPmm, long? optionalInactiveThreshold);
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
        
        internal ConsumerContext(StreamContext sc, ConsumerInfo ci)
        {
            stateLock = new object();
            streamCtx = sc;
            ordered = false;
            originalOrderedCc = null;
            subscribeSubject = null;
            ConsumerName = ci.Name;
            unorderedBindPso = PullSubscribeOptions.FastBindTo(streamCtx.StreamName, ci.Name);
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
            subscribeSubject = Validator.ValidateSubject(originalOrderedCc.FilterSubject, false);
            unorderedBindPso = null;

        }

        public IJetStreamSubscription Subscribe(EventHandler<MsgHandlerEventArgs> messageHandler, PullMessageManager optionalPmm, long? optionalInactiveThreshold) {
            PullSubscribeOptions pso;
            if (ordered) {
                if (lastConsumer != null) {
                    highestSeq = Math.Max(highestSeq, lastConsumer.pmm.LastStreamSeq);
                }
                ConsumerConfiguration cc = lastConsumer == null
                    ? originalOrderedCc
                    : streamCtx.js.ConsumerConfigurationForOrdered(originalOrderedCc, highestSeq, null, null, optionalInactiveThreshold);
                pso = new OrderedPullSubscribeOptionsBuilder(streamCtx.StreamName, cc).Build();
            }
            else {
                pso = unorderedBindPso;
            }

            if (messageHandler == null) {
                return (JetStreamPullSubscription) streamCtx.js.CreateSubscription(subscribeSubject, null, pso, null, null, false, optionalPmm);
            }
            
            return (JetStreamPullAsyncSubscription)streamCtx.js.CreateSubscription(subscribeSubject, null, pso, null, messageHandler, false, optionalPmm);
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

        public string ConsumerName { get; private set; }

        public ConsumerInfo GetConsumerInfo()
        {
            cachedConsumerInfo = streamCtx.jsm.GetConsumerInfo(streamCtx.StreamName, cachedConsumerInfo.Name);
            ConsumerName = cachedConsumerInfo.Name;
            return cachedConsumerInfo;
        }

        public ConsumerInfo GetCachedConsumerInfo()
        {
            return cachedConsumerInfo;
        }

        public Msg Next(int maxWaitMillis = DefaultExpiresInMillis)
        {
            if (maxWaitMillis < MinExpiresMills) 
            {
                throw new ArgumentException($"Max wait must be at least {MinExpiresMills} milliseconds.");
            }

            MessageConsumerBase mcb = null;
            lock (stateLock)
            {
                CheckState();

                try
                {
                    long inactiveThreshold = maxWaitMillis * 110 / 100; // 10% longer than the wait
                    mcb = new MessageConsumerBase(cachedConsumerInfo);
                    mcb.InitSub(Subscribe(null, null, null));
                    mcb.pullImpl.Pull(PullRequestOptions.Builder(1)
                        .WithExpiresIn(maxWaitMillis - JetStreamPullSubscription.ExpireAdjustment)
                        .Build(), false, null);
                    TrackConsume(mcb);
                }
                catch (Exception)
                {
                    if (mcb != null)
                    {
                        try
                        {
                            mcb.Dispose();
                        }
                        catch (Exception) { /* ignore */ }
                    }
                    return null;
                }
            }

            // intentionally outside of lock because I don't want to 
            // hold it while fetching the next  message
            try
            {
                return ((JetStreamPullSubscription)mcb.sub).NextMessage(maxWaitMillis);
            }
            catch (NATSTimeoutException)
            {
                return null;
            }
            finally
            {
                try
                {
                    mcb.Finished = true;
                    mcb.Dispose();
                }
                catch (Exception) { /* ignore */ }
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
                return TrackConsume(new MessageConsumer(this, cachedConsumerInfo, consumeOptions ?? DefaultConsumeOptions, handler));
            }
        }
    }
}
