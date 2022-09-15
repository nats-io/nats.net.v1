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

using System.Collections.Concurrent;
using System.Collections.Generic;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    internal class CachedStreamInfo
    {
        // currently the only thing we care about caching is the allowDirect setting
        internal bool AllowDirect { get; }

        internal CachedStreamInfo(StreamInfo si)
        {
            AllowDirect = si.Config.AllowDirect;
        }        
    }

    public class JetStreamBase
    {
        private readonly ConcurrentDictionary<string, CachedStreamInfo> cachedStreamInfoDictionary =
            new ConcurrentDictionary<string, CachedStreamInfo>();

        private readonly bool _server290orLater;
        
        public string Prefix { get; }
        public JetStreamOptions JetStreamOptions { get; }
        public IConnection Conn { get; }
        public int Timeout { get; }

        protected JetStreamBase(IConnection connection, JetStreamOptions options)
        {
            Conn = connection;
            JetStreamOptions = options ?? JetStreamOptions.DefaultJsOptions;
            Prefix = JetStreamOptions.Prefix;
            Timeout = JetStreamOptions.RequestTimeout.Millis;
            _server290orLater = Conn.ServerInfo.IsSameOrNewerThanVersion("2.9.0");
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
            string name = Validator.EmptyAsNull(config.Name);
            if (!string.IsNullOrWhiteSpace(name) && !_server290orLater)
            {
                throw ClientExDetail.JsConsumerCantUseNameBefore290.Instance();
            }
            
            string durable = Validator.EmptyAsNull(config.Durable);

            string consumerName = name ?? durable;

            string subj;

            if (consumerName == null) // just use old template
            {
                subj = string.Format(JetStreamConstants.JsapiConsumerCreate, streamName);
            }
            else if (_server290orLater)
            {
                string fs = Validator.EmptyAsNull(config.FilterSubject);
                if (fs == null || fs.Equals(">"))
                {
                    subj = string.Format(JetStreamConstants.JsapiConsumerCreateV290, streamName, consumerName);
                }
                else
                {
                    subj = string.Format(JetStreamConstants.JsapiConsumerCreateV290WithFilter, streamName, consumerName, fs);
                }
            }
            else // server is old and consumerName must be durable since name was checked for JsConsumerCantUseNameBefore290
            {
                subj = string.Format(JetStreamConstants.JsapiDurableCreate, streamName, durable);
            }

            var ccr = new ConsumerCreateRequest(streamName, config);
            var m = RequestResponseRequired(subj, ccr.Serialize(), Timeout);
            return new ConsumerInfo(m, true);
        }

        internal StreamInfo GetStreamInfoInternal(string streamName, StreamInfoOptions options)
        {
            byte[] payload = options == null ? null : options.Serialize();
            string subj = string.Format(JetStreamConstants.JsapiStreamInfo, streamName);
            Msg m = RequestResponseRequired(subj, payload, Timeout);
            return CreateAndCacheStreamInfoThrowOnError(streamName, m);
        }

        internal StreamInfo CreateAndCacheStreamInfoThrowOnError(string streamName, Msg resp) {
            return CacheStreamInfo(streamName, new StreamInfo(resp, true));
        }

        internal StreamInfo CacheStreamInfo(string streamName, StreamInfo si) {
            cachedStreamInfoDictionary[streamName] = new CachedStreamInfo(si);
            return si;
        }
        
        internal IList<StreamInfo> CacheStreamInfo(IList<StreamInfo> list) {
            foreach (StreamInfo si in list)
            {
                cachedStreamInfoDictionary[si.Config.Name] = new CachedStreamInfo(si);
            }
            return list;
        }

        internal IList<string> GetStreamNamesBySubjectFilterInternal(string subjectFilter)
        {
            byte[] body = JsonUtils.SimpleMessageBody(ApiConstants.Subject, subjectFilter); 
            Msg resp = RequestResponseRequired(JetStreamConstants.JsapiStreamNames, body, Timeout);
            StreamNamesReader snr = new StreamNamesReader();
            snr.Process(resp);
            return snr.Strings;
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
 
        internal CachedStreamInfo GetCachedStreamInfo(string streamName) {
            CachedStreamInfo csi;
            cachedStreamInfoDictionary.TryGetValue(streamName, out csi);
            if (csi != null) {
                return csi;
            }
            GetStreamInfoInternal(streamName, null);
            cachedStreamInfoDictionary.TryGetValue(streamName, out csi);
            return csi;
        }
    }
}
