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
using System.Threading.Tasks;
using NATS.Client.Internals;
using static NATS.Client.JetStream.BaseConsumeOptions;
using static NATS.Client.JetStream.ConsumeOptions;
using static NATS.Client.JetStream.JetStreamPullSubscription;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
    /// </summary>
    internal class ConsumerContext : IConsumerContext
    {
        private readonly StreamContext streamContext;
        private readonly JetStream js;
        private readonly PullSubscribeOptions bindPso;
        private ConsumerInfo lastConsumerInfo;

        public string ConsumerName => lastConsumerInfo.Name;
        
        internal ConsumerContext(StreamContext sc, ConsumerInfo ci)
        {
            streamContext = sc;
            js = new JetStream(streamContext.jsm.Conn, streamContext.jsm.JetStreamOptions);
            bindPso = PullSubscribeOptions.BindTo(streamContext.StreamName, ci.Name);
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

        public Msg Next(int maxWaitMillis = DefaultExpiresInMillis) 
        {
            if (maxWaitMillis < MinExpiresMills) 
            {
                throw new ArgumentException($"Max wait must be at least {MinExpiresMills} milliseconds.");
            }
            return new NextSub(js, bindPso, maxWaitMillis).Next();
        }

        public IFetchConsumer FetchMessages(int maxMessages) {
            return Fetch(FetchConsumeOptions.Builder().WithMaxMessages(maxMessages).Build());
        }

        public IFetchConsumer FetchBytes(int maxBytes) {
            return Fetch(FetchConsumeOptions.Builder().WithMaxBytes(maxBytes).Build());
        }

        public IFetchConsumer Fetch(FetchConsumeOptions fetchConsumeOptions) {
            Validator.Required(fetchConsumeOptions, "Fetch Consume Options");
            return new FetchConsumer(new SubscriptionMaker(js, bindPso), fetchConsumeOptions, lastConsumerInfo);
        }

        public IIterableConsumer StartIterate(ConsumeOptions consumeOptions = null) {
            return new IterableConsumer(new SubscriptionMaker(js, bindPso), consumeOptions ?? DefaultConsumeOptions, lastConsumerInfo);
        }

        public IMessageConsumer StartConsume(EventHandler<MsgHandlerEventArgs> handler, ConsumeOptions consumeOptions = null) {
            Validator.Required(handler, "Msg Handler");
            return new MessageConsumer(new SubscriptionMaker(js, bindPso), handler, consumeOptions ?? DefaultConsumeOptions, lastConsumerInfo);
        }
    }

    internal class NextSub
    {
        private int maxWaitMillis;
        private JetStreamPullSubscription sub; 

        public NextSub(IJetStream js, PullSubscribeOptions pso, int maxWaitMillis)
        {
            sub = (JetStreamPullSubscription)new SubscriptionMaker(js, pso).MakeSubscription();
            this.maxWaitMillis = maxWaitMillis;
            sub.pullImpl.Pull(PullRequestOptions.Builder(1).WithExpiresIn(maxWaitMillis - ExpireAdjustment).Build(), false, null);
        }

        internal Msg Next()
        {
            try
            {
                return sub.NextMessage(maxWaitMillis);
            }
            catch (NATSTimeoutException)
            {
                return null;
            }
            finally
            {
                Task.Run(() =>
                {
                    try
                    {
                        sub.Unsubscribe();
                    }
                    catch (Exception)
                    {
                        // intentionally ignored, nothing we can do anyway
                    }
                });
            }
        }
    }
    
    internal class SubscriptionMaker
    {
        private readonly IJetStream js;
        private readonly PullSubscribeOptions pso;
        private readonly string subscribeSubject;

        public SubscriptionMaker(IJetStream js, PullSubscribeOptions pso, string subscribeSubject = null)
        {
            this.js = js;
            this.pso = pso;
            this.subscribeSubject = subscribeSubject;
        }

        public IJetStreamSubscription MakeSubscription(EventHandler<MsgHandlerEventArgs> handler = null) {
            if (handler == null) {
                return (JetStreamPullSubscription)js.PullSubscribe(subscribeSubject, pso);
            }
            return (JetStreamPullAsyncSubscription)js.PullSubscribeAsync(subscribeSubject, handler, pso);
        }
    }
}
