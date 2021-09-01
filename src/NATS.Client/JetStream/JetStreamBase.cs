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

using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public class JetStreamBase
    {
        public string Prefix { get; }
        public JetStreamOptions JetStreamOptions { get; }
        public IConnection Conn { get; }
        public int Timeout { get; }

        protected JetStreamBase(IConnection connection, JetStreamOptions options)
        {
            Conn = connection;
            JetStreamOptions = options ?? JetStreamOptions.Builder().Build();
            Prefix = JetStreamOptions.Prefix;
            Timeout = (int) JetStreamOptions.RequestTimeout.Millis;
        }
        
        // ----------------------------------------------------------------------------------------------------
        // Management that is also needed by regular context
        // ----------------------------------------------------------------------------------------------------
        internal ConsumerInfo GetConsumerInfoInternal(string streamName, string consumer) {
            string subj = string.Format(JetStreamConstants.JsapiConsumerInfo, streamName, consumer);
            var m = RequestResponseRequired(subj, null, Timeout);
            return new ConsumerInfo(m, true);
        }

        internal ConsumerInfo AddOrUpdateConsumerInternal(string streamName, ConsumerConfiguration config)
        {
            string subj = string.IsNullOrWhiteSpace(config.Durable)
                ? string.Format(JetStreamConstants.JsapiConsumerCreate, streamName)
                : string.Format(JetStreamConstants.JsapiDurableCreate, streamName, config.Durable);

            var ccr = new ConsumerCreateRequest(streamName, config);
            var m = RequestResponseRequired(subj, ccr.Serialize(), Timeout);
            return new ConsumerInfo(m, true);
        }

        // ----------------------------------------------------------------------------------------------------
        // Request Utils
        // ----------------------------------------------------------------------------------------------------
        internal string PrependPrefix(string subject) => Prefix + subject;
    
        public Msg RequestResponseRequired(string subject, byte[] bytes, int timeout)
        {
            Msg msg = Conn.Request(PrependPrefix(subject), bytes, timeout);
            if (msg == null)
            {
                throw new NATSJetStreamException("Timeout or no response waiting for NATS JetStream server");
            }

            return msg;
        }
    }
}
