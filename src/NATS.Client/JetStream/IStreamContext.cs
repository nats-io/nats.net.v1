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

using System.Collections.Generic;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// The Stream Context provide a set of operations for managing the stream
    /// and its contents and for managing consumers.
    /// </summary>
    public interface IStreamContext
    {
        /// <summary>
        /// Gets the stream name that was used to create the context.
        /// </summary>
        /// <returns>the stream name</returns>
        string StreamName { get; }

        /// <summary>
        /// Gets information about the stream for this context.
        /// Does not retrieve any optional data.
        /// See the overloaded version that accepts StreamInfoOptions
        /// </summary>
        /// <returns>stream information</returns>
        StreamInfo GetStreamInfo();

        /// <summary>
        /// Gets information about the stream for this context.
        /// </summary>
        /// <param name="options">the stream info options. If null, request will not return any optional data.</param>
        /// <returns>stream information</returns>
        StreamInfo GetStreamInfo(StreamInfoOptions options);

        /// <summary>
        /// Purge stream messages
        /// </summary>
        /// <returns>PurgeResponse the purge response</returns>
        PurgeResponse Purge();

        /// <summary>
        /// Purge messages for a specific subject
        /// </summary>
        /// <param name="options">the purge options</param>
        /// <returns>PurgeResponse the purge response</returns>
        PurgeResponse Purge(PurgeOptions options);

        /// <summary>
        /// Create a consumer context for on the context's stream and specific named consumer.
        /// Verifies that the consumer exists.
        /// </summary>
        /// <param name="consumerName">the name of the consumer</param>
        /// <returns>an instance of IConsumerContext</returns>
        IConsumerContext GetConsumerContext(string consumerName);

        /// <summary>
        /// Create an ordered consumer context for the context's stream.
        /// </summary>
        /// <param name="config">the configuration for the ordered consumer</param>
        /// <returns>an instance of IConsumerContext</returns>
        IConsumerContext CreateOrUpdateConsumer(ConsumerConfiguration config);

        /// <summary>
        /// Management function to creates a consumer on this stream.
        /// </summary>
        /// <param name="config">the consumer configuration to use.</param>
        /// <returns>consumer information.</returns>
        IOrderedConsumerContext CreateOrderedConsumer(OrderedConsumerConfiguration config);

        /// <summary>
        /// Management function to deletes a consumer.
        /// </summary>
        /// <param name="consumerName">the name of the consumer.</param>
        /// <returns>true if the delete succeeded</returns>
        bool DeleteConsumer(string consumerName);

        /// <summary>
        /// Gets the info for an existing consumer.
        /// </summary>
        /// <param name="consumerName">the name of the consumer.</param>
        /// <returns>consumer information</returns>
        ConsumerInfo GetConsumerInfo(string consumerName);

        /// <summary>
        /// Return a list of consumers by name
        /// </summary>
        /// <returns>The list of names</returns>
        IList<string> GetConsumerNames();

        /// <summary>
        /// Return a list of ConsumerInfo objects.
        /// </summary>
        /// <returns>The list of ConsumerInfo</returns>
        IList<ConsumerInfo> GetConsumers();

        /// <summary>
        /// Get MessageInfo for the message with the exact sequence in the stream.
        /// </summary>
        /// <param name="seq">the sequence number of the message</param>
        /// <returns>The MessageInfo</returns>
        MessageInfo GetMessage(ulong seq);

        /// <summary>
        /// Get MessageInfo for the last message of the subject.
        /// </summary>
        /// <param name="subject">the subject to get the last message for.</param>
        /// <returns>The MessageInfo</returns>
        MessageInfo GetLastMessage(string subject);

        /// <summary>
        /// Get MessageInfo for the first message of the subject.
        /// </summary>
        /// <param name="subject">the subject to get the first message for.</param>
        /// <returns>The MessageInfo</returns>
        MessageInfo GetFirstMessage(string subject);

        /// <summary>
        /// Get MessageInfo for the message of the message sequence
        /// is equal to or greater the requested sequence for the subject.
        /// </summary>
        /// <param name="seq">the first possible sequence number of the message</param>
        /// <param name="subject">the subject to get the next message for.</param>
        /// <returns>The MessageInfo</returns>
        MessageInfo GetNextMessage(ulong seq, string subject);

        /// <summary>
        /// Deletes a message, overwriting the message data with garbage
        /// This can be considered an expensive (time-consuming) operation, but is more secure.
        /// </summary>
        /// <param name="seq">the sequence number of the message</param>
        /// <returns>true if the delete succeeded</returns>
        bool DeleteMessage(ulong seq);

        /// <summary>
        /// Deletes a message, optionally erasing the content of the message.
        /// </summary>
        /// <param name="seq">the sequence number of the message</param>
        /// <param name="erase">whether to erase the message (overwriting with garbage) or only mark it as erased.</param>
        /// <returns>true if the delete succeeded</returns>
        bool DeleteMessage(ulong seq, bool erase);
    }
}