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

        public string Prefix { get; }
        public JetStreamOptions JetStreamOptions { get; }
        public IConnection Conn { get; }
        public int Timeout { get; }

        private bool? _consumerCreate290Available;
        private bool? _multipleSubjectFilter210Available;

        protected bool ConsumerCreate290Available()
        {
            if (!_consumerCreate290Available.HasValue)
            {
                _consumerCreate290Available = Conn.ServerInfo.IsSameOrNewerThanVersion("2.9.0") &&
                                              !JetStreamOptions.IsOptOut290ConsumerCreate;
            }
            return _consumerCreate290Available.Value;
        }

        protected bool MultipleSubjectFilter210Available()
        {
            if (!_multipleSubjectFilter210Available.HasValue)
            {
                _multipleSubjectFilter210Available = Conn.ServerInfo.IsSameOrNewerThanVersion("2.9.99");
            }
            return _multipleSubjectFilter210Available.Value;
        }

        protected JetStreamBase(IConnection connection, JetStreamOptions options)
        {
            Conn = connection;
            JetStreamOptions = options ?? JetStreamOptions.DefaultJsOptions;
            Prefix = JetStreamOptions.Prefix;
            Timeout = JetStreamOptions.RequestTimeout?.Millis ?? Conn.Opts.Timeout;
            
        }

        internal static ServerInfo ServerInfoOrException(IConnection conn)
        {
            ServerInfo si = conn.ServerInfo;
            if (si == null)
            {
                throw new NATSConnectionClosedException();
            }

            return si;
        }
            
        // ----------------------------------------------------------------------------------------------------
        // Management that is also needed by regular context
        // ----------------------------------------------------------------------------------------------------
        internal ConsumerInfo GetConsumerInfoInternal(string streamName, string consumer) {
            string subj = string.Format(JetStreamConstants.JsapiConsumerInfo, streamName, consumer);
            var m = RequestResponseRequired(subj, null, Timeout);
            return new ConsumerInfo(m, true);
        }

        internal ConsumerInfo CreateConsumerInternal(string streamName, ConsumerConfiguration config)
        {
            // ConsumerConfiguration validates that name and durable are the same if both are supplied.
            string consumerName = Validator.EmptyAsNull(config.Name);
            if (consumerName != null && !ConsumerCreate290Available())
            {
                throw ClientExDetail.JsConsumerCreate290NotAvailable.Instance();
            }

            bool hasMultipleFilterSubjects = config.HasMultipleFilterSubjects;
            
            // seems strange that this could happen, but checking anyway...
            if (hasMultipleFilterSubjects && !MultipleSubjectFilter210Available()) {
                throw ClientExDetail.JsMultipleFilterSubjects210NotAvailable.Instance();
            }

            string durable = Validator.EmptyAsNull(config.Durable);
            string subj;
            if (ConsumerCreate290Available() && !hasMultipleFilterSubjects)
            {
                if (consumerName == null) 
                {
                    // if both consumerName and durable are null, generate a name
                    consumerName = durable ?? GenerateConsumerName();
                }

                string fs = config.FilterSubject;  // we've already determined not multiple so this gives us 1 or null
                if (fs == null || fs.Equals(">"))
                {
                    subj = string.Format(JetStreamConstants.JsapiConsumerCreateV290, streamName, consumerName);
                }
                else
                {
                    subj = string.Format(JetStreamConstants.JsapiConsumerCreateV290WithFilter, streamName, consumerName, fs);
                }
            }
            else if (durable == null) 
            {
                subj = string.Format(JetStreamConstants.JsapiConsumerCreate, streamName);
            }
            else 
            {
                subj = string.Format(JetStreamConstants.JsapiDurableCreate, streamName, durable);
            }

            var ccr = new ConsumerCreateRequest(streamName, config);
            var m = RequestResponseRequired(subj, ccr.Serialize(), Timeout);
            return new ConsumerInfo(m, true);
        }

        internal string GenerateConsumerName()
        {
            return Nuid.NextGlobalSequence();
        }
        
        internal ConsumerConfiguration NextOrderedConsumerConfiguration(
            ConsumerConfiguration originalCc,
            ulong lastStreamSeq,
            string newDeliverSubject)
        {
            return ConsumerConfiguration.Builder(originalCc)
                .WithDeliverPolicy(DeliverPolicy.ByStartSequence)
                .WithDeliverSubject(newDeliverSubject)
                .WithStartSequence(Math.Max(1, lastStreamSeq + 1))
                .WithStartTime(DateTime.MinValue) // clear start time in case it was originally set
                .Build();
        }

        internal StreamInfo GetStreamInfoInternal(string streamName, StreamInfoOptions options)
        {
            string subj = string.Format(JetStreamConstants.JsapiStreamInfo, streamName);
            StreamInfoReader sir = new StreamInfoReader();
            while (sir.HasMore())
            {
                Msg resp = RequestResponseRequired(subj, sir.NextJson(options), Timeout);
                sir.Process(resp);
            }
            return CacheStreamInfo(streamName, sir.StreamInfo);
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

        internal IList<string> GetStreamNamesInternal(string subjectFilter)
        {
            StreamNamesReader snr = new StreamNamesReader();
            while (snr.HasMore()) {
                Msg m = RequestResponseRequired(JetStreamConstants.JsapiStreamNames, snr.NextJson(subjectFilter), Timeout);
                snr.Process(m);
            }
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
