// Copyright 2021 The NATS Authors
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
using System.Threading.Tasks;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public class JetStreamManagementAsync : JetStreamBase, IJetStreamManagementAsync
    {
        internal JetStreamManagementAsync(IConnection connection, JetStreamOptions options) : base(connection, options) {}

        public async Task<AccountStatistics> GetAccountStatisticsAsync()
        {
            Msg m = await RequestResponseRequiredAsync(JetStreamConstants.JsapiAccountInfo, null, Timeout);
            return new AccountStatistics(m, true);
        }

        public async Task<StreamInfo> AddStreamAsync(StreamConfiguration config)
            => await AddOrUpdateStreamAsync(config, JetStreamConstants.JsapiStreamCreate);

        public async Task<StreamInfo> UpdateStreamAsync(StreamConfiguration config)
            => await AddOrUpdateStreamAsync(config, JetStreamConstants.JsapiStreamUpdate);

        private async Task<StreamInfo> AddOrUpdateStreamAsync(StreamConfiguration config, string addUpdateTemplate)
        {
            Validator.ValidateNotNull(config, nameof(config));

            if (string.IsNullOrWhiteSpace(config.Name)) {
                throw new ArgumentException("Configuration must have a valid stream name");
            }

            string subj = string.Format(addUpdateTemplate, config.Name);
            Msg m = await RequestResponseRequiredAsync(subj, config.Serialize(), Timeout);
            return CreateAndCacheStreamInfoThrowOnError(config.Name, m);
        }

        public async Task<bool> DeleteStreamAsync(string streamName)
        {
            Validator.ValidateStreamName(streamName, true);
            string subj = string.Format(JetStreamConstants.JsapiStreamDelete, streamName);
            Msg m = await RequestResponseRequiredAsync(subj, null, Timeout);
            return new SuccessApiResponse(m, true).Success;
        }

        public async Task<StreamInfo> GetStreamInfoAsync(string streamName)
        {
            Validator.ValidateStreamName(streamName, true);
            return await GetStreamInfoInternalAsync(streamName, null);
        }

        public async Task<StreamInfo> GetStreamInfoAsync(string streamName, StreamInfoOptions options)
        {
            Validator.ValidateStreamName(streamName, true);
            return await GetStreamInfoInternalAsync(streamName, options);
        }

        public async Task<PurgeResponse> PurgeStreamAsync(string streamName)
        {
            Validator.ValidateStreamName(streamName, true);
            string subj = string.Format(JetStreamConstants.JsapiStreamPurge, streamName);
            Msg m = await RequestResponseRequiredAsync(subj, null, Timeout);
            return new PurgeResponse(m, true);
        }

        public async Task<PurgeResponse> PurgeStreamAsync(string streamName, PurgeOptions options)
        {
            Validator.ValidateStreamName(streamName, true);
            string subj = string.Format(JetStreamConstants.JsapiStreamPurge, streamName);
            Msg m = await RequestResponseRequiredAsync(subj, options.Serialize(), Timeout);
            return new PurgeResponse(m, true);
        }

        public async Task<ConsumerInfo> AddOrUpdateConsumerAsync(string streamName, ConsumerConfiguration config)
        {
            Validator.ValidateStreamName(streamName, true);
            Validator.ValidateNotNull(config, nameof(config));
            Validator.ValidateNotNull(config.Durable, nameof(config.Durable)); // durable name is required when creating consumers
            return await AddOrUpdateConsumerInternalAsync(streamName, config);
        }

        public async Task<bool> DeleteConsumerAsync(string streamName, string consumer)
        {
            Validator.ValidateStreamName(streamName, true);
            Validator.ValidateNotNull(consumer, nameof(consumer));
            string subj = string.Format(JetStreamConstants.JsapiConsumerDelete, streamName, consumer);
            Msg m = await RequestResponseRequiredAsync(subj, null, Timeout);
            return new SuccessApiResponse(m, true).Success;
        }

        public async Task<ConsumerInfo> GetConsumerInfoAsync(string streamName, string consumer)
        {
            Validator.ValidateStreamName(streamName, true);
            Validator.ValidateNotNull(consumer, nameof(consumer));
            return await GetConsumerInfoInternalAsync(streamName, consumer);
        }

        public async Task<IList<string>> GetConsumerNamesAsync(string streamName)
        {
            ConsumerNamesReader cnr = new ConsumerNamesReader();
            while (cnr.HasMore()) {
                string subj = string.Format(JetStreamConstants.JsapiConsumerNames, streamName);
                Msg m = await RequestResponseRequiredAsync(subj, cnr.NextJson(), Timeout);
                cnr.Process(m);
            }
            return cnr.Strings;
        }

        public async Task<IList<ConsumerInfo>> GetConsumersAsync(string streamName)
        {
            ConsumerListReader clr = new ConsumerListReader();
            while (clr.HasMore()) {
                string subj = string.Format(JetStreamConstants.JsapiConsumerList, streamName);
                Msg m = await RequestResponseRequiredAsync(subj, clr.NextJson(), Timeout);
                clr.Process(m);
            }
            return clr.Consumers;
        }

        public async Task<IList<string>> GetStreamNamesAsync()
        {
            return await GetStreamNamesInternalAsync(null);
        }

        public async Task<IList<string>> GetStreamNamesAsync(string subjectFilter)
        {
            return await GetStreamNamesInternalAsync(subjectFilter);
        }

        public async Task<IList<StreamInfo>> GetStreamsAsync()
        {
            return await GetStreamsAsync(null);
        }

        public async Task<IList<StreamInfo>> GetStreamsAsync(string subjectFilter)
        {
            StreamListReader slr = new StreamListReader();
            while (slr.HasMore()) {
                Msg m = await RequestResponseRequiredAsync(JetStreamConstants.JsapiStreamList, slr.NextJson(subjectFilter), Timeout);
                slr.Process(m);
            }
            return CacheStreamInfo(slr.Streams);
        }

        public async Task<MessageInfo> GetMessageAsync(string streamName, ulong sequence)
        {
            return await _GetMessageAsync(streamName, MessageGetRequest.ForSequence(sequence));
        }

        public async Task<MessageInfo> GetLastMessageAsync(string streamName, string subject)
        {
            return await _GetMessageAsync(streamName, MessageGetRequest.LastForSubject(subject));
        }

        public async Task<MessageInfo> GetFirstMessageAsync(string streamName, string subject)
        {
            return await _GetMessageAsync(streamName, MessageGetRequest.FirstForSubject(subject));
        }

        public async Task<MessageInfo> GetNextMessageAsync(string streamName, ulong sequence, string subject)
        {
            return await _GetMessageAsync(streamName, MessageGetRequest.NextForSubject(sequence, subject));
        }

        internal async Task<MessageInfo> _GetMessageAsync(string streamName, MessageGetRequest messageGetRequest)
        {
            Validator.ValidateStreamName(streamName, true);
            CachedStreamInfo csi = GetCachedStreamInfo(streamName);
            if (csi.AllowDirect) {
                string subj;
                byte[] payload;
                if (messageGetRequest.IsLastBySubject) {
                    subj = string.Format(JetStreamConstants.JsapiDirectGetLast, streamName, messageGetRequest.LastBySubject);
                    payload = null;
                }
                else {
                    subj = string.Format(JetStreamConstants.JsapiDirectGet, streamName);
                    payload = messageGetRequest.Serialize();
                }
                Msg resp = await RequestResponseRequiredAsync(subj, payload, Timeout);
                if (resp.HasStatus) {
                    throw new NATSJetStreamException(Error.Convert(resp.Status));
                }
                return new MessageInfo(resp, streamName, true, true);
            }
            else {
                string subj = string.Format(JetStreamConstants.JsapiMsgGet, streamName);
                Msg m = await RequestResponseRequiredAsync(subj, messageGetRequest.Serialize(), Timeout);
                return new MessageInfo(m, streamName, false, true);
            }
        }

        public async Task<bool> DeleteMessageAsync(string streamName, ulong sequence)
        {
            return await DeleteMessageAsync(streamName, sequence, true);
        }

        public async Task<bool> DeleteMessageAsync(string streamName, ulong sequence, bool erase)
        {
            Validator.ValidateStreamName(streamName, true);
            string subj = string.Format(JetStreamConstants.JsapiMsgDelete, streamName);
            byte[] mdr = new MessageDeleteRequest(sequence, erase).Serialize();
            Msg m = await RequestResponseRequiredAsync(subj, mdr, Timeout);
            return new SuccessApiResponse(m, true).Success;
        }
    }
}
