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
    /// <remarks>
    /// A ConsumerContext is created by the various IJetStream.ConsumerContext(...) APIs.
    /// </remarks>
    public interface IConsumerContext
    {
        /// <summary>
        /// Gets the consumer name that was used to create the context.
        /// </summary>
        /// <returns>the consumer name</returns>
        string ConsumerName { get; }

        /// <summary>
        /// Gets information about the consumer behind this subscription.
        /// </summary>
        /// <returns>consumer information</returns>
        ConsumerInfo GetConsumerInfo();

        /// <summary>
        /// Gets information about the consumer behind this subscription.
        /// This returns the last read version of Consumer Info, which could technically be out of date.
        /// </summary>
        /// <returns>consumer information</returns>
        ConsumerInfo GetCachedConsumerInfo();

        /// <summary>
        /// Read the next message with provide max wait
        /// </summary>
        /// <param name="maxWaitMillis">optional max wait value in milliseconds. Defaults to {@value BaseConsumeOptions#DEFAULT_EXPIRES_IN_MS}</param>
        /// <returns>the next message or null if the max wait expires</returns>
        Msg Next(int maxWaitMillis = BaseConsumeOptions.DefaultExpiresInMillis);

        /// <summary>
        /// Create a one use Fetch Consumer using all defaults other than the number of messages. <see cref="IFetchConsumer"/>
        /// </summary>
        /// <param name="maxMessages">the maximum number of message to consume</param>
        /// <returns>the IFetchConsumer instance</returns>
        IFetchConsumer FetchMessages(int maxMessages);

        /// <summary>
        /// Create a one use Fetch Consumer using all defaults other than the number of bytes. <see cref="IFetchConsumer"/>
        /// </summary>
        /// <param name="maxBytes">the maximum number of bytes to consume</param>
        /// <returns>the IFetchConsumer instance</returns>
        IFetchConsumer FetchBytes(int maxBytes);

        /// <summary>
        /// Create a one use Fetch Consumer with complete custom consume options. <see cref="IFetchConsumer"/>
        /// </summary>
        /// <param name="fetchConsumeOptions">the custom fetch consume options. See FetchConsumeOptions</param>
        /// <returns>the IFetchConsumer instance</returns>
        IFetchConsumer Fetch(FetchConsumeOptions fetchConsumeOptions);

        /// <summary>
        /// Create a long-running IterableConsumer with optional custom ConsumeOptions.<see cref="IIterableConsumer"/> and <see cref="ConsumeOptions"/>
        /// IIterableConsumer requires the developer call nextMessage.
        /// </summary>
        /// <param name="consumeOptions">optional custom consume options</param>
        /// <returns>the IIterableConsumer instance</returns>
        IIterableConsumer CreateIterable(ConsumeOptions consumeOptions = null);

        /// <summary>
        /// Create a long-running MessageConsumer with a handler and optional custom ConsumeOptions.<see cref="IIterableConsumer"/> and <see cref="ConsumeOptions"/>
        /// </summary>
        /// <param name="handler">the MessageHandler used for receiving messages.</param>
        /// <param name="consumeOptions">optional custom consume options</param>
        /// <returns>the IMessageConsumer instance</returns>
        IMessageConsumer CreateConsumer(EventHandler<MsgHandlerEventArgs> handler, ConsumeOptions consumeOptions = null);
    }
}
