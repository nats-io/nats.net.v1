using System;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Implementation for the IOrderedConsumerContext
    /// </summary>
    internal class OrderedConsumerContext : IOrderedConsumerContext
    {
        private ConsumerContext impl;
        
        internal OrderedConsumerContext(StreamContext streamContext, OrderedConsumerConfiguration config) {
            impl = new ConsumerContext(streamContext, config);
        }

        public Msg Next(int maxWaitMillis = BaseConsumeOptions.DefaultExpiresInMillis)
        {
            return impl.Next(maxWaitMillis);
        }

        public IFetchConsumer FetchMessages(int maxMessages)
        {
            return impl.FetchMessages(maxMessages);
        }

        public IFetchConsumer FetchBytes(int maxBytes)
        {
            return impl.FetchBytes(maxBytes);
        }

        public IFetchConsumer Fetch(FetchConsumeOptions fetchConsumeOptions)
        {
            return impl.Fetch(fetchConsumeOptions);
        }

        public IIterableConsumer Iterate(ConsumeOptions consumeOptions = null)
        {
            return impl.Iterate(consumeOptions);
        }

        public IMessageConsumer Consume(EventHandler<MsgHandlerEventArgs> handler, ConsumeOptions consumeOptions = null)
        {
            return impl.Consume(handler, consumeOptions);
        }
    }
}