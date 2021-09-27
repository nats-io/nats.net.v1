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
            return new StreamInfo(m, true);
        }
        
        public bool DeleteStream(string streamName)
        {
            Validator.ValidateNotNull(streamName, nameof(streamName));
            string subj = string.Format(JetStreamConstants.JsapiStreamDelete, streamName);
            Msg m = RequestResponseRequired(subj, null, Timeout);
            return new SuccessApiResponse(m, true).Success;
        }

        public StreamInfo GetStreamInfo(string streamName)
        {
            Validator.ValidateNotNull(streamName, nameof(streamName));
            string subj = string.Format(JetStreamConstants.JsapiStreamInfo, streamName);
            Msg m = RequestResponseRequired(subj, null, Timeout);
            return new StreamInfo(m, true);
        }

        public PurgeResponse PurgeStream(string streamName)
        {
            Validator.ValidateNotNull(streamName, nameof(streamName));
            string subj = string.Format(JetStreamConstants.JsapiStreamPurge, streamName);
            Msg m = RequestResponseRequired(subj, null, Timeout);
            return new PurgeResponse(m, true);
        }

        public ConsumerInfo AddOrUpdateConsumer(string streamName, ConsumerConfiguration config)
        {
            Validator.ValidateStreamName(streamName, true);
            Validator.ValidateNotNull(config, nameof(config));
            Validator.ValidateNotNull(config.Durable, nameof(config.Durable)); // durable name is required when creating consumers
            return AddOrUpdateConsumerInternal(streamName, config);
        }

        public bool DeleteConsumer(string streamName, string consumer)
        {
            Validator.ValidateNotNull(streamName, nameof(streamName));
            Validator.ValidateNotNull(consumer, nameof(consumer));
            string subj = string.Format(JetStreamConstants.JsapiConsumerDelete, streamName, consumer);
            Msg m = RequestResponseRequired(subj, null, Timeout);
            return new SuccessApiResponse(m, true).Success;
        }

        public ConsumerInfo GetConsumerInfo(string streamName, string consumer)
        {
            Validator.ValidateNotNull(streamName, nameof(streamName));
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
            StreamNamesReader snr = new StreamNamesReader();
            while (snr.HasMore()) {
                Msg m = RequestResponseRequired(JetStreamConstants.JsapiStreamNames, snr.NextJson(), Timeout);
                snr.Process(m);
            }
            return snr.Strings;
        }

        public IList<StreamInfo> GetStreams()
        {
            StreamListReader slr = new StreamListReader();
            while (slr.HasMore()) {
                Msg m = RequestResponseRequired(JetStreamConstants.JsapiStreamList, slr.NextJson(), Timeout);
                slr.Process(m);
            }
            return slr.Streams;
        }

        public MessageInfo GetMessage(string streamName, ulong sequence)
        {
            Validator.ValidateNotNull(streamName, nameof(streamName));
            string subj = string.Format(JetStreamConstants.JsapiMsgGet, streamName);
            byte[] bytes = JsonUtils.SimpleMessageBody(ApiConstants.Seq, sequence);
            Msg m = RequestResponseRequired(subj, bytes, Timeout);
            return new MessageInfo(m, true);
        }

        public bool DeleteMessage(string streamName, ulong sequence)
        {
            Validator.ValidateNotNull(streamName, nameof(streamName));
            string subj = string.Format(JetStreamConstants.JsapiMsgDelete, streamName);
            byte[] bytes = JsonUtils.SimpleMessageBody(ApiConstants.Seq, sequence);
            Msg m = RequestResponseRequired(subj, bytes, Timeout);
            return new SuccessApiResponse(m, true).Success;
        }
    }
}
