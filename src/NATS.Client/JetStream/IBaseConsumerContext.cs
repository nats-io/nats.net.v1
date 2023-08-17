using System;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Base class for IConsumerContext implementations
    /// </summary>
    public interface IBaseConsumerContext
    {
        /// <summary>
        /// Read the next message with optional provided max wait
        /// </summary>
        /// <param name="maxWaitMillis">optional max wait value in milliseconds. Defaults to {@value BaseConsumeOptions#DEFAULT_EXPIRES_IN_MS}</param>
        /// <returns>the next message or null if the max wait expires</returns>
        Msg Next(int maxWaitMillis = BaseConsumeOptions.DefaultExpiresInMillis);

        /// <summary>
        /// Start a one use Fetch Consumer using all defaults other than the number of messages. <see cref="IFetchConsumer"/>
        /// </summary>
        /// <param name="maxMessages">the maximum number of message to consume</param>
        /// <returns>the IFetchConsumer instance</returns>
        IFetchConsumer FetchMessages(int maxMessages);

        /// <summary>
        /// Start a one use Fetch Consumer using all defaults other than the number of bytes. <see cref="IFetchConsumer"/>
        /// </summary>
        /// <param name="maxBytes">the maximum number of bytes to consume</param>
        /// <returns>the IFetchConsumer instance</returns>
        IFetchConsumer FetchBytes(int maxBytes);

        /// <summary>
        /// Start a one use Fetch Consumer with complete custom consume options. <see cref="IFetchConsumer"/>
        /// </summary>
        /// <param name="fetchConsumeOptions">the custom fetch consume options. See FetchConsumeOptions</param>
        /// <returns>the IFetchConsumer instance</returns>
        IFetchConsumer Fetch(FetchConsumeOptions fetchConsumeOptions);

        /// <summary>
        /// Start a long-running IterableConsumer with optional custom ConsumeOptions.<see cref="IIterableConsumer"/> and <see cref="ConsumeOptions"/>
        /// IIterableConsumer requires the developer call nextMessage.
        /// </summary>
        /// <param name="consumeOptions">optional custom consume options</param>
        /// <returns>the IIterableConsumer instance</returns>
        IIterableConsumer Iterate(ConsumeOptions consumeOptions = null);

        /// <summary>
        /// Start a long-running MessageConsumer with a handler and optional custom ConsumeOptions.<see cref="IIterableConsumer"/> and <see cref="ConsumeOptions"/>
        /// </summary>
        /// <param name="handler">the MessageHandler used for receiving messages.</param>
        /// <param name="consumeOptions">optional custom consume options</param>
        /// <returns>the IMessageConsumer instance</returns>
        IMessageConsumer Consume(EventHandler<MsgHandlerEventArgs> handler, ConsumeOptions consumeOptions = null);
    }
}