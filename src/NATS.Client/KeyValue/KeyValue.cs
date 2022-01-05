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

using System;
using System.Collections.Generic;
using System.Text;
using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATS.Client.KeyValue
{
    public class KeyValue : IKeyValue
    {
        internal JetStream.JetStream js;
        internal IJetStreamManagement jsm;
        internal string StreamName { get; }
        internal string StreamSubject { get; }
        internal string DefaultKeyPrefix { get; }
        internal string PublishKeyPrefix { get; }
        
        internal KeyValue(IConnection connection, string bucketName, KeyValueOptions kvo) {
            BucketName = Validator.ValidateKvBucketNameRequired(bucketName);
            StreamName = KeyValueUtil.ToStreamName(BucketName);
            StreamSubject = KeyValueUtil.ToStreamSubject(BucketName);
            DefaultKeyPrefix = KeyValueUtil.ToKeyPrefix(bucketName);
            if (kvo == null)
            {
                js = new JetStream.JetStream(connection, null);
                jsm = new JetStreamManagement(connection, null);
                PublishKeyPrefix = DefaultKeyPrefix;
            }
            else
            {
                js = new JetStream.JetStream(connection, kvo.JSOptions);
                jsm = new JetStreamManagement(connection, kvo.JSOptions);
                PublishKeyPrefix = kvo.FeaturePrefix ?? DefaultKeyPrefix;
            }
        }

        public string BucketName { get; }
        
        internal string DefaultKeySubject(string key)
        {
            return DefaultKeyPrefix + key;
        }
        
        internal string PublishKeySubject(string key)
        {
            return PublishKeyPrefix + key;
        }

        public KeyValueEntry Get(string key)
        {
            return GetLastMessage(Validator.ValidateNonWildcardKvKeyRequired(key));
        }

        internal KeyValueEntry GetLastMessage(string key)
        {
            string subj = string.Format(JetStreamConstants.JsapiMsgGet, StreamName);
            byte[] bytes = MessageGetRequest.LastBySubjectBytes(DefaultKeySubject(key));
            Msg resp = js.RequestResponseRequired(subj, bytes, JetStreamOptions.DefaultTimeout.Millis);
            MessageInfo mi = new MessageInfo(resp, false);
            if (mi.HasError)
            {
                if (mi.ApiErrorCode == JetStreamConstants.JsNoMessageFoundErr)
                {
                    return null; // run of the mill key not found
                }

                mi.ThrowOnHasError();
            }

            return new KeyValueEntry(mi);
        }

        public ulong Put(string key, byte[] value)
        {
            return _publishWithNonWildcardKey(key, value, null).Seq;
        }

        public ulong Put(string key, string value) => Put(key, Encoding.UTF8.GetBytes(value));

        public ulong Put(string key, long value) => Put(key, Encoding.UTF8.GetBytes(value.ToString()));
        
        public ulong Create(string key, byte[] value)
        {
            try
            {
                return Update(key, value, 0);
            }
            catch (NATSJetStreamException e)
            {
                if (e.ApiErrorCode == JetStreamConstants.JsWrongLastSequence)
                {
                    // must check if the last message for this subject is a delete or purge
                    KeyValueEntry kve = GetLastMessage(key);
                    if (kve != null && !kve.Operation.Equals(KeyValueOperation.Put)) {
                        return Update(key, value, kve.Revision);
                    }
                }

                throw;
            }
        }

        public ulong Update(string key, byte[] value, ulong expectedRevision)
        {
            MsgHeader h = new MsgHeader 
            {
                [JetStreamConstants.ExpLastSubjectSeqHeader] = expectedRevision.ToString()
            };
            return _publishWithNonWildcardKey(key, value, h).Seq;
        }

        public void Delete(string key) => _publishWithNonWildcardKey(key, null, KeyValueUtil.DeleteHeaders);

        public void Purge(string key) => _publishWithNonWildcardKey(key, null, KeyValueUtil.PurgeHeaders);
        
        public KeyValueWatchSubscription Watch(string key, IKeyValueWatcher watcher, params KeyValueWatchOption[] watchOptions)
        {
            Validator.ValidateKvKeyWildcardAllowedRequired(key);
            Validator.ValidateNotNull(watcher, "Watcher is required");
            return new KeyValueWatchSubscription(this, key, watcher, watchOptions);
        }

        public KeyValueWatchSubscription WatchAll(IKeyValueWatcher watcher, params KeyValueWatchOption[] watchOptions)
        {
            Validator.ValidateNotNull(watcher, "Watcher is required");
            return new KeyValueWatchSubscription(this, ">", watcher, watchOptions);
        }

        private PublishAck _publishWithNonWildcardKey(string key, byte[] data, MsgHeader h) {
            Validator.ValidateNonWildcardKvKeyRequired(key);
            return js.Publish(new Msg(PublishKeySubject(key), h, data));
        }

        public IList<string> Keys()
        {
            IList<string> list = new List<string>();
            VisitSubject(DefaultKeySubject(">"), DeliverPolicy.LastPerSubject, true, false, m => {
                KeyValueOperation op = KeyValueUtil.GetOperation(m.Header, KeyValueOperation.Put);
                if (op.Equals(KeyValueOperation.Put)) {
                    list.Add(new BucketAndKey(m).Key);
                }
            });
            return list;
        }

        public IList<KeyValueEntry> History(string key)
        {
            IList<KeyValueEntry> list = new List<KeyValueEntry>();
            VisitSubject(DefaultKeySubject(key), DeliverPolicy.All, false, true, m => {
                list.Add(new KeyValueEntry(m));
            });
            return list;
        }

        public void PurgeDeletes()
        {
            IList<string> list = new List<string>();
            VisitSubject(KeyValueUtil.ToStreamSubject(BucketName), DeliverPolicy.LastPerSubject, true, false, m => {
                KeyValueOperation op = KeyValueUtil.GetOperation(m.Header, KeyValueOperation.Put);
                if (!op.Equals(KeyValueOperation.Put)) {
                    list.Add(new BucketAndKey(m).Key);
                }
            });

            foreach (string key in list)
            {
                jsm.PurgeStream(StreamName, PurgeOptions.WithSubject(DefaultKeySubject(key)));
            }
        }

        public KeyValueStatus Status()
        {
            return new KeyValueStatus(jsm.GetStreamInfo(KeyValueUtil.ToStreamName(BucketName)));
        }
 
        private void VisitSubject(string subject, DeliverPolicy deliverPolicy, bool headersOnly, bool ordered, Action<Msg> action) {
            PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                .WithOrdered(ordered)
                .WithConfiguration(
                    ConsumerConfiguration.Builder()
                        .WithAckPolicy(AckPolicy.None)
                        .WithDeliverPolicy(deliverPolicy)
                        .WithHeadersOnly(headersOnly)
                        .Build())
                .Build();
            
            IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(subject, pso);
            try
            {
                int timeout = 5000; // give plenty of time for the first. Also used to quit loop
                while (timeout > 0)
                {
                    Msg m = sub.NextMessage(timeout);
                    action.Invoke(m);
                    if (m.MetaData.NumPending == 0)
                    {
                        timeout = 0;
                    }
                    else
                    {
                        timeout = 100; // the rest should come pretty quick
                    }
                }
            }
            catch (NATSTimeoutException)
            {
                /* no more messages */
            }
            finally
            {
                sub.Unsubscribe();
            }
        }
    }
}