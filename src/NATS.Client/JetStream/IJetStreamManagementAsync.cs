// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Generic;
using System.Threading.Tasks;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// This is the JetStream management async API to programmatically create, delete,
    /// and update various JetStream objects.
    /// </summary>
    public interface IJetStreamManagementAsync
    {
        /// <summary>
        /// Gets the account statistics for the logged in account.
        /// <returns>account statistics</returns>
        /// </summary>
        Task<AccountStatistics> GetAccountStatisticsAsync();

        /// <summary>
        /// Loads or creates a stream.
        /// </summary>
        /// <param name="config">The stream configuration to use.</param>
        /// <returns>Stream information</returns>
        Task<StreamInfo> AddStreamAsync(StreamConfiguration config);

        /// <summary>
        /// Updates an existing stream.
        /// </summary>
        /// <param name="config">The stream configuration to use.</param>
        /// <returns>Stream information</returns>
        Task<StreamInfo> UpdateStreamAsync(StreamConfiguration config);

        /// <summary>
        /// Deletes an existing stream.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <returns>true if the delete succeeded. Usually throws a NATSJetStreamException otherwise</returns>
        Task<bool> DeleteStreamAsync(string streamName);

        /// <summary>
        /// Get information about a stream.
        /// Does not retrieve any optional data.
        /// See the overloaded version that accepts StreamInfoOptions
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <returns>Stream information</returns>
        Task<StreamInfo> GetStreamInfoAsync(string streamName);

        /// <summary>
        /// Get information about a stream, and include optional information
        /// as defined in the StreamInfoOptions.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <param name="options">the stream info options. If null, request will not return any optional data.</param>
        /// <returns>Stream information</returns>
        Task<StreamInfo> GetStreamInfoAsync(string streamName, StreamInfoOptions options);

        /// <summary>
        /// Purges all messages in a stream.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <returns>The result of the purge.</returns>
        Task<PurgeResponse> PurgeStreamAsync(string streamName);

        /// <summary>
        /// Purges all messages in a stream.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <param name="options">The purge options.</param>
        /// <returns>The result of the purge.</returns>
        Task<PurgeResponse> PurgeStreamAsync(string streamName, PurgeOptions options);

        /// <summary>
        /// Adds or updates a consumer.
        /// </summary>
        /// <param name="streamName">The name of the stream the consumer is attached to.</param>
        /// <param name="config">The consumer configuration to use.</param>
        /// <returns></returns>
        Task<ConsumerInfo> AddOrUpdateConsumerAsync(string streamName, ConsumerConfiguration config);

        /// <summary>
        /// Deletes a consumer.
        /// </summary>
        /// <param name="streamName">The name of the stream the consumer is attached to.</param>
        /// <param name="consumer">The name of the consumer.</param>
        /// <returns>True if the consumer was deleted.</returns>
        Task<bool> DeleteConsumerAsync(string streamName, string consumer);

        /// <summary>
        /// Gets information for an existing consumer.
        /// </summary>
        /// <param name="streamName">The name of the stream the consumer is attached to.</param>
        /// <param name="consumer">The name of the consumer.</param>
        /// <returns>Consumer information</returns>
        Task<ConsumerInfo> GetConsumerInfoAsync(string streamName, string consumer);

        /// <summary>
        /// Gets all consumers attached to a stream.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <returns>An array of consumer names.</returns>
        Task<IList<string>> GetConsumerNamesAsync(string streamName);

        /// <summary>
        /// Get consumer information for all consumers on a stream.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <returns>An array of consumer information objects.</returns>
        Task<IList<ConsumerInfo>> GetConsumersAsync(string streamName);

        /// <summary>
        /// Gets the names of all streams.
        /// </summary>
        /// <returns>An array of stream names.</returns>
        Task<IList<string>> GetStreamNamesAsync();

        /// <summary>
        /// Get a list of stream names that have subjects matching the subject filter.
        /// </summary>
        /// <param name="subjectFilter">The subject. Wildcards are allowed.</param>
        /// <returns></returns>
        Task<IList<string>> GetStreamNamesAsync(string subjectFilter);

        /// <summary>
        /// Gets stream information about all streams.
        /// </summary>
        /// <returns>An array of stream information objects.</returns>
        Task<IList<StreamInfo>> GetStreamsAsync();

        /// <summary>
        /// Gets stream information about all streams that have subjects matching the filter.
        /// </summary>
        /// <returns>An array of stream information objects.</returns>
        Task<IList<StreamInfo>> GetStreamsAsync(string subjectFilter);

        /// <summary>
        /// Get MessageInfo for the message with the exact sequence in the stream.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <param name="sequence">The stream sequence number of the message.</param>
        /// <returns>Message information.</returns>
        Task<MessageInfo> GetMessageAsync(string streamName, ulong sequence);

        /// <summary>
        /// Get MessageInfo for the last message of the subject.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <param name="subject">The subject to get the last message for.</param>
        /// <returns>Message information.</returns>
        Task<MessageInfo> GetLastMessageAsync(string streamName, string subject);

        /// <summary>
        /// Get MessageInfo for the first message of the subject.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <param name="subject">The subject to get the first message for.</param>
        /// <returns>Message information.</returns>
        Task<MessageInfo> GetFirstMessageAsync(string streamName, string subject);

        /// <summary>
        /// Get MessageInfo for the message of the message sequence
        /// is equal to or greater the requested sequence for the subject.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <param name="sequence">The first possible sequence number of the message.</param>
        /// <param name="subject">The subject to get the next message for.</param>
        /// <returns>Message information.</returns>
        Task<MessageInfo> GetNextMessageAsync(string streamName, ulong sequence, string subject);

        /// <summary>
        /// Deletes a message, overwriting the message data with garbage
        /// This can be considered an expensive (time consuming) operation, but is more secure.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <param name="sequence">The stream sequence number of the message.</param>
        /// <returns>True if the message was deleted.</returns>
        Task<bool> DeleteMessageAsync(string streamName, ulong sequence);

        /// <summary>
        /// Deletes a message, optionally erasing the content of the message.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <param name="sequence">The stream sequence number of the message.</param>
        /// <param name="erase">Whether to erase the message (overwriting with garbage) or only mark it as erased.</param>
        /// <returns>True if the message was deleted.</returns>
        Task<bool> DeleteMessageAsync(string streamName, ulong sequence, bool erase);
    }
}
