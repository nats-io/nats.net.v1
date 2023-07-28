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
using System.Collections.Generic;
using NATS.Client.Internals;
using static NATS.Client.JetStream.PullSubscribeOptions;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
    /// </summary>
    internal class StreamContext : IStreamContext
    {
        internal readonly JetStreamManagement jsm;
        internal readonly JetStream js;

        public string StreamName { get; }

        internal StreamContext(string streamName, JetStream js, IConnection connection, JetStreamOptions jsOptions)
        {
            StreamName = streamName;
            this.js = js ?? new JetStream(connection, jsOptions);
            jsm = new JetStreamManagement(connection, jsOptions);
            jsm.GetStreamInfo(StreamName); // this is just verifying that the stream exists
        }

        public StreamInfo GetStreamInfo()
        {
            return jsm.GetStreamInfo(StreamName);
        }

        public StreamInfo GetStreamInfo(StreamInfoOptions options)
        {
            return jsm.GetStreamInfo(StreamName, options);
        }

        public PurgeResponse Purge()
        {
            return jsm.PurgeStream(StreamName);
        }

        public PurgeResponse Purge(PurgeOptions options)
        {
            return jsm.PurgeStream(StreamName, options);
        }

        public IConsumerContext CreateConsumerContext(string consumerName)
        {
            return new ConsumerContext(this, jsm.GetConsumerInfo(StreamName, consumerName));
        }

        public IConsumerContext CreateOrUpdateConsumer(ConsumerConfiguration config)
        {
            return new ConsumerContext(this, jsm.AddOrUpdateConsumer(StreamName, config));
        }

        public bool DeleteConsumer(string consumerName)
        {
            return jsm.DeleteConsumer(StreamName, consumerName);
        }

        public ConsumerInfo GetConsumerInfo(string consumerName)
        {
            return jsm.GetConsumerInfo(StreamName, consumerName);
        }

        public IList<string> GetConsumerNames()
        {
            return jsm.GetConsumerNames(StreamName);
        }

        public IList<ConsumerInfo> GetConsumers()
        {
            return jsm.GetConsumers(StreamName);
        }

        public MessageInfo GetMessage(ulong seq)
        {
            return jsm.GetMessage(StreamName, seq);
        }

        public MessageInfo GetLastMessage(string subject)
        {
            return jsm.GetLastMessage(StreamName, subject);
        }

        public MessageInfo GetFirstMessage(string subject)
        {
            return jsm.GetFirstMessage(StreamName, subject);
        }

        public MessageInfo GetNextMessage(ulong seq, string subject)
        {
            return jsm.GetNextMessage(StreamName, seq, subject);
        }

        public bool DeleteMessage(ulong seq)
        {
            return jsm.DeleteMessage(StreamName, seq);
        }

        public bool DeleteMessage(ulong seq, bool erase)
        {
            return jsm.DeleteMessage(StreamName, seq, erase);
        }

        public IIterableConsumer CreateOrderedIterable(OrderedConsumerConfiguration config, ConsumeOptions consumeOptions = null)
        {
            Validator.Required(config, "Ordered Consumer Config");
            ConsumerConfiguration cc = GetBackingConsumerConfiguration(config);
            PullSubscribeOptions pso = new OrderedPullSubscribeOptionsBuilder(StreamName, cc).Build();
            return new IterableConsumer(new SubscriptionMaker(js, pso, cc.FilterSubject), 
                consumeOptions == null ? ConsumeOptions.DefaultConsumeOptions : consumeOptions, null);
        }

        public IMessageConsumer CreateOrderedConsumer(OrderedConsumerConfiguration config, EventHandler<MsgHandlerEventArgs> handler,
            ConsumeOptions consumeOptions = null)
        {
            Validator.Required(config, "Ordered Consumer Config");
            Validator.Required(handler, "Msg Handler");
            ConsumerConfiguration cc = GetBackingConsumerConfiguration(config);
            PullSubscribeOptions pso = new OrderedPullSubscribeOptionsBuilder(StreamName, cc).Build();
            return new MessageConsumer(new SubscriptionMaker(js, pso, cc.FilterSubject), handler, 
                consumeOptions ?? ConsumeOptions.DefaultConsumeOptions, null);
        }
        
        private ConsumerConfiguration GetBackingConsumerConfiguration(OrderedConsumerConfiguration occ) {
            return ConsumerConfiguration.Builder()
                .WithName(JetStreamBase.GenerateConsumerName())
                .WithFilterSubject(occ.FilterSubject)
                .WithDeliverPolicy(occ.DeliverPolicy)
                .WithStartSequence(occ.StartSequence)
                .WithStartTime(occ.StartTime)
                .WithReplayPolicy(occ.ReplayPolicy)
                .WithHeadersOnly(occ.HeadersOnly)
                .Build();
        }    
    }
    
    internal class OrderedPullSubscribeOptionsBuilder : PullSubscribeOptionsBuilder {
        public OrderedPullSubscribeOptionsBuilder(String streamName, ConsumerConfiguration config) {
            WithStream(streamName);
            WithConfiguration(config);
            _ordered = true;
        }
    }
}
