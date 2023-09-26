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
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public class JetStreamManagement : JetStreamBase, IJetStreamManagement
    {
        internal JetStreamManagement(IConnection connection, JetStreamOptions options) : base(connection, options) {}
        
        public AccountStatistics GetAccountStatistics()
        {
            Msg m = RequestResponseRequired(JetStreamConstants.JsapiAccountInfo, null, Timeout);
            return new AccountStatistics(m, true);
        }

        public StreamInfo AddStream(StreamConfiguration config)
            => AddOrUpdateStream(config, JetStreamConstants.JsapiStreamCreate);

        public StreamInfo UpdateStream(StreamConfiguration config)
            => AddOrUpdateStream(config, JetStreamConstants.JsapiStreamUpdate);

        private StreamInfo AddOrUpdateStream(StreamConfiguration config, string addUpdateTemplate)
        {
            Validator.ValidateNotNull(config, nameof(config));
            
            if (string.IsNullOrWhiteSpace(config.Name)) {
                throw new ArgumentException("Configuration must have a valid stream name");
            }

            string subj = string.Format(addUpdateTemplate, config.Name);
            Msg m = RequestResponseRequired(subj, config.Serialize(), Timeout);
            return CreateAndCacheStreamInfoThrowOnError(config.Name, m);
        }
        
        public bool DeleteStream(string streamName)
        {
            Validator.ValidateStreamName(streamName, true);
            string subj = string.Format(JetStreamConstants.JsapiStreamDelete, streamName);
            Msg m = RequestResponseRequired(subj, null, Timeout);
            return new SuccessApiResponse(m, true).Success;
        }

        public StreamInfo GetStreamInfo(string streamName)
        {
            Validator.ValidateStreamName(streamName, true);
            return GetStreamInfoInternal(streamName, null);
        }

        public StreamInfo GetStreamInfo(string streamName, StreamInfoOptions options)
        {
            Validator.ValidateStreamName(streamName, true);
            return GetStreamInfoInternal(streamName, options);
        }

        public PurgeResponse PurgeStream(string streamName)
        {
            Validator.ValidateStreamName(streamName, true);
            string subj = string.Format(JetStreamConstants.JsapiStreamPurge, streamName);
            Msg m = RequestResponseRequired(subj, null, Timeout);
            return new PurgeResponse(m, true);
        }

        public PurgeResponse PurgeStream(string streamName, PurgeOptions options)
        {
            Validator.ValidateStreamName(streamName, true);
            Validator.ValidateNotNull(options, nameof(options));
            string subj = string.Format(JetStreamConstants.JsapiStreamPurge, streamName);
            Msg m = RequestResponseRequired(subj, options.Serialize(), Timeout);
            return new PurgeResponse(m, true);
        }

        public ConsumerInfo AddOrUpdateConsumer(string streamName, ConsumerConfiguration config)
        {
            Validator.ValidateStreamName(streamName, true);
            Validator.ValidateNotNull(config, nameof(config));
            return CreateConsumerInternal(streamName, config);
        }

        public bool DeleteConsumer(string streamName, string consumer)
        {
            Validator.ValidateStreamName(streamName, true);
            Validator.ValidateNotNull(consumer, nameof(consumer));
            string subj = string.Format(JetStreamConstants.JsapiConsumerDelete, streamName, consumer);
            Msg m = RequestResponseRequired(subj, null, Timeout);
            return new SuccessApiResponse(m, true).Success;
        }

        public ConsumerInfo GetConsumerInfo(string streamName, string consumer)
        {
            Validator.ValidateStreamName(streamName, true);
            Validator.ValidateNotNull(consumer, nameof(consumer));
            return GetConsumerInfoInternal(streamName, consumer);
        }

        public IList<string> GetConsumerNames(string streamName)
        {
            ConsumerNamesReader cnr = new ConsumerNamesReader();
            while (cnr.HasMore()) {
                string subj = string.Format(JetStreamConstants.JsapiConsumerNames, streamName);
                Msg m = RequestResponseRequired(subj, cnr.NextJson(), Timeout);
                cnr.Process(m);
            }
            return cnr.Strings;
        }

        public IList<ConsumerInfo> GetConsumers(string streamName)
        {
            ConsumerListReader clr = new ConsumerListReader();
            while (clr.HasMore()) {
                string subj = string.Format(JetStreamConstants.JsapiConsumerList, streamName);
                Msg m = RequestResponseRequired(subj, clr.NextJson(), Timeout);
                clr.Process(m);
            }
            return clr.Consumers;
        }

        public IList<string> GetStreamNames()
        {
            return GetStreamNamesInternal(null);
        }

        public IList<string> GetStreamNames(string subjectFilter)
        {
            return GetStreamNamesInternal(subjectFilter);
        }

        public IList<StreamInfo> GetStreams()
        {
            return GetStreams(null);
        }

        public IList<StreamInfo> GetStreams(string subjectFilter)
        {
            StreamListReader slr = new StreamListReader();
            while (slr.HasMore()) {
                Msg m = RequestResponseRequired(JetStreamConstants.JsapiStreamList, slr.NextJson(subjectFilter), Timeout);
                slr.Process(m);
            }
            return CacheStreamInfo(slr.Streams);
        }

        public MessageInfo GetMessage(string streamName, ulong sequence)
        {
            return _GetMessage(streamName, MessageGetRequest.ForSequence(sequence));
        }

        public MessageInfo GetLastMessage(string streamName, string subject)
        {
            return _GetMessage(streamName, MessageGetRequest.LastForSubject(subject));
        }

        public MessageInfo GetFirstMessage(string streamName, string subject)
        {
            return _GetMessage(streamName, MessageGetRequest.FirstForSubject(subject));
        }

        public MessageInfo GetNextMessage(string streamName, ulong sequence, string subject)
        {
            return _GetMessage(streamName, MessageGetRequest.NextForSubject(sequence, subject));
        }

        internal MessageInfo _GetMessage(string streamName, MessageGetRequest messageGetRequest)
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
                Msg resp = RequestResponseRequired(subj, payload, Timeout);
                if (resp.HasStatus) {
                    throw new NATSJetStreamException(Error.Convert(resp.Status));
                }
                return new MessageInfo(resp, streamName, true, true);
            }
            else {
                string subj = string.Format(JetStreamConstants.JsapiMsgGet, streamName);
                Msg m = RequestResponseRequired(subj, messageGetRequest.Serialize(), Timeout);
                return new MessageInfo(m, streamName, false, true);
            }
        }

        public bool DeleteMessage(string streamName, ulong sequence)
        {
            return DeleteMessage(streamName, sequence, true);
        }

        public bool DeleteMessage(string streamName, ulong sequence, bool erase)
        {
            Validator.ValidateStreamName(streamName, true);
            string subj = string.Format(JetStreamConstants.JsapiMsgDelete, streamName);
            byte[] mdr = new MessageDeleteRequest(sequence, erase).Serialize();
            Msg m = RequestResponseRequired(subj, mdr, Timeout);
            return new SuccessApiResponse(m, true).Success;
        }
    }
}
