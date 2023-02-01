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
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public class JetStreamManagementAsync : JetStreamBase, IJetStreamManagementAsync
    {
        internal JetStreamManagementAsync(IConnection connection, JetStreamOptions options) : base(connection, options) {}

        public async Task<AccountStatistics> GetAccountStatisticsAsync(CancellationToken cancellationToken = default)
        {
            Msg m = await RequestResponseRequiredAsync(JetStreamConstants.JsapiAccountInfo, null, Timeout, cancellationToken).ConfigureAwait(false);
            return new AccountStatistics(m, true);
        }

        public async Task<StreamInfo> AddStreamAsync(StreamConfiguration config, CancellationToken cancellationToken = default)
            => await AddOrUpdateStreamAsync(config, JetStreamConstants.JsapiStreamCreate, cancellationToken).ConfigureAwait(false);

        public async Task<StreamInfo> UpdateStreamAsync(StreamConfiguration config, CancellationToken cancellationToken = default)
            => await AddOrUpdateStreamAsync(config, JetStreamConstants.JsapiStreamUpdate, cancellationToken).ConfigureAwait(false);

        private async Task<StreamInfo> AddOrUpdateStreamAsync(StreamConfiguration config, string addUpdateTemplate, CancellationToken cancellationToken)
        {
            Validator.ValidateNotNull(config, nameof(config));

            if (string.IsNullOrWhiteSpace(config.Name)) {
                throw new ArgumentException("Configuration must have a valid stream name");
            }

            string subj = string.Format(addUpdateTemplate, config.Name);
            Msg m = await RequestResponseRequiredAsync(subj, config.Serialize(), Timeout, cancellationToken).ConfigureAwait(false);
            return CreateAndCacheStreamInfoThrowOnError(config.Name, m);
        }

        public async Task<bool> DeleteStreamAsync(string streamName, CancellationToken cancellationToken = default)
        {
            Validator.ValidateStreamName(streamName, true);
            string subj = string.Format(JetStreamConstants.JsapiStreamDelete, streamName);
            Msg m = await RequestResponseRequiredAsync(subj, null, Timeout, cancellationToken).ConfigureAwait(false);
            return new SuccessApiResponse(m, true).Success;
        }

        public async Task<StreamInfo> GetStreamInfoAsync(string streamName, CancellationToken cancellationToken = default)
        {
            Validator.ValidateStreamName(streamName, true);
            return await GetStreamInfoInternalAsync(streamName, null, cancellationToken).ConfigureAwait(false);
        }

        public async Task<StreamInfo> GetStreamInfoAsync(string streamName, StreamInfoOptions options, CancellationToken cancellationToken = default)
        {
            Validator.ValidateStreamName(streamName, true);
            return await GetStreamInfoInternalAsync(streamName, options, cancellationToken).ConfigureAwait(false);
        }

        public async Task<PurgeResponse> PurgeStreamAsync(string streamName, CancellationToken cancellationToken = default)
        {
            Validator.ValidateStreamName(streamName, true);
            string subj = string.Format(JetStreamConstants.JsapiStreamPurge, streamName);
            Msg m = await RequestResponseRequiredAsync(subj, null, Timeout, cancellationToken).ConfigureAwait(false);
            return new PurgeResponse(m, true);
        }

        public async Task<PurgeResponse> PurgeStreamAsync(string streamName, PurgeOptions options, CancellationToken cancellationToken = default)
        {
            Validator.ValidateStreamName(streamName, true);
            string subj = string.Format(JetStreamConstants.JsapiStreamPurge, streamName);
            Msg m = await RequestResponseRequiredAsync(subj, options.Serialize(), Timeout, cancellationToken).ConfigureAwait(false);
            return new PurgeResponse(m, true);
        }

        public async Task<ConsumerInfo> AddOrUpdateConsumerAsync(string streamName, ConsumerConfiguration config, CancellationToken cancellationToken = default)
        {
            Validator.ValidateStreamName(streamName, true);
            Validator.ValidateNotNull(config, nameof(config));
            Validator.ValidateNotNull(config.Durable, nameof(config.Durable)); // durable name is required when creating consumers
            return await AddOrUpdateConsumerInternalAsync(streamName, config, cancellationToken).ConfigureAwait(false);
        }

        public async Task<bool> DeleteConsumerAsync(string streamName, string consumer, CancellationToken cancellationToken = default)
        {
            Validator.ValidateStreamName(streamName, true);
            Validator.ValidateNotNull(consumer, nameof(consumer));
            string subj = string.Format(JetStreamConstants.JsapiConsumerDelete, streamName, consumer);
            Msg m = await RequestResponseRequiredAsync(subj, null, Timeout, cancellationToken).ConfigureAwait(false);
            return new SuccessApiResponse(m, true).Success;
        }

        public async Task<ConsumerInfo> GetConsumerInfoAsync(string streamName, string consumer, CancellationToken cancellationToken = default)
        {
            Validator.ValidateStreamName(streamName, true);
            Validator.ValidateNotNull(consumer, nameof(consumer));
            return await GetConsumerInfoInternalAsync(streamName, consumer, cancellationToken).ConfigureAwait(false);
        }

        public async Task<IList<string>> GetConsumerNamesAsync(string streamName, CancellationToken cancellationToken = default)
        {
            ConsumerNamesReader cnr = new ConsumerNamesReader();
            while (cnr.HasMore()) {
                string subj = string.Format(JetStreamConstants.JsapiConsumerNames, streamName);
                Msg m = await RequestResponseRequiredAsync(subj, cnr.NextJson(), Timeout, cancellationToken).ConfigureAwait(false);
                cnr.Process(m);
            }
            return cnr.Strings;
        }

        public async Task<IList<ConsumerInfo>> GetConsumersAsync(string streamName, CancellationToken cancellationToken = default)
        {
            ConsumerListReader clr = new ConsumerListReader();
            while (clr.HasMore()) {
                string subj = string.Format(JetStreamConstants.JsapiConsumerList, streamName);
                Msg m = await RequestResponseRequiredAsync(subj, clr.NextJson(), Timeout, cancellationToken).ConfigureAwait(false);
                clr.Process(m);
            }
            return clr.Consumers;
        }

        public async Task<IList<string>> GetStreamNamesAsync(CancellationToken cancellationToken = default)
        {
            return await GetStreamNamesInternalAsync(null, cancellationToken).ConfigureAwait(false);
        }

        public async Task<IList<string>> GetStreamNamesAsync(string subjectFilter, CancellationToken cancellationToken = default)
        {
            return await GetStreamNamesInternalAsync(subjectFilter, cancellationToken).ConfigureAwait(false);
        }

        public async Task<IList<StreamInfo>> GetStreamsAsync(CancellationToken cancellationToken = default)
        {
            return await GetStreamsAsync(null, cancellationToken).ConfigureAwait(false);
        }

        public async Task<IList<StreamInfo>> GetStreamsAsync(string subjectFilter, CancellationToken cancellationToken = default)
        {
            StreamListReader slr = new StreamListReader();
            while (slr.HasMore()) {
                Msg m = await RequestResponseRequiredAsync(JetStreamConstants.JsapiStreamList, slr.NextJson(subjectFilter), Timeout, cancellationToken).ConfigureAwait(false);
                slr.Process(m);
            }
            return CacheStreamInfo(slr.Streams);
        }

        public async Task<MessageInfo> GetMessageAsync(string streamName, ulong sequence, CancellationToken cancellationToken = default)
        {
            return await _GetMessageAsync(streamName, MessageGetRequest.ForSequence(sequence), cancellationToken).ConfigureAwait(false);
        }

        public async Task<MessageInfo> GetLastMessageAsync(string streamName, string subject, CancellationToken cancellationToken = default)
        {
            return await _GetMessageAsync(streamName, MessageGetRequest.LastForSubject(subject), cancellationToken).ConfigureAwait(false);
        }

        public async Task<MessageInfo> GetFirstMessageAsync(string streamName, string subject, CancellationToken cancellationToken = default)
        {
            return await _GetMessageAsync(streamName, MessageGetRequest.FirstForSubject(subject), cancellationToken).ConfigureAwait(false);
        }

        public async Task<MessageInfo> GetNextMessageAsync(string streamName, ulong sequence, string subject, CancellationToken cancellationToken = default)
        {
            return await _GetMessageAsync(streamName, MessageGetRequest.NextForSubject(sequence, subject), cancellationToken).ConfigureAwait(false);
        }

        internal async Task<MessageInfo> _GetMessageAsync(string streamName, MessageGetRequest messageGetRequest, CancellationToken cancellationToken)
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
                Msg resp = await RequestResponseRequiredAsync(subj, payload, Timeout, cancellationToken).ConfigureAwait(false);
                if (resp.HasStatus) {
                    throw new NATSJetStreamException(Error.Convert(resp.Status));
                }
                return new MessageInfo(resp, streamName, true, true);
            }
            else {
                string subj = string.Format(JetStreamConstants.JsapiMsgGet, streamName);
                Msg m = await RequestResponseRequiredAsync(subj, messageGetRequest.Serialize(), Timeout, cancellationToken).ConfigureAwait(false);
                return new MessageInfo(m, streamName, false, true);
            }
        }

        public async Task<bool> DeleteMessageAsync(string streamName, ulong sequence, CancellationToken cancellationToken = default)
        {
            return await DeleteMessageAsync(streamName, sequence, true, cancellationToken).ConfigureAwait(false);
        }

        public async Task<bool> DeleteMessageAsync(string streamName, ulong sequence, bool erase, CancellationToken cancellationToken = default)
        {
            Validator.ValidateStreamName(streamName, true);
            string subj = string.Format(JetStreamConstants.JsapiMsgDelete, streamName);
            byte[] mdr = new MessageDeleteRequest(sequence, erase).Serialize();
            Msg m = await RequestResponseRequiredAsync(subj, mdr, Timeout, cancellationToken).ConfigureAwait(false);
            return new SuccessApiResponse(m, true).Success;
        }
    }
}
