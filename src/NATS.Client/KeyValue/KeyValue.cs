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
        internal string RawKeyPrefix { get; }
        internal string PubSubKeyPrefix { get; }
        
        internal KeyValue(IConnection connection, string bucketName, KeyValueOptions kvo) {
            BucketName = Validator.ValidateKvBucketNameRequired(bucketName);
            StreamName = KeyValueUtil.ToStreamName(BucketName);
            StreamSubject = KeyValueUtil.ToStreamSubject(BucketName);
            RawKeyPrefix = KeyValueUtil.ToKeyPrefix(bucketName);
            if (kvo == null)
            {
                js = new JetStream.JetStream(connection, null);
                jsm = new JetStreamManagement(connection, null);
                PubSubKeyPrefix = RawKeyPrefix;
            }
            else
            {
                js = new JetStream.JetStream(connection, kvo.JSOptions);
                jsm = new JetStreamManagement(connection, kvo.JSOptions);
                if (kvo.JSOptions.IsDefaultPrefix)
                {
                    PubSubKeyPrefix = RawKeyPrefix;
                }
                else
                {
                    PubSubKeyPrefix = kvo.JSOptions.Prefix + RawKeyPrefix;
                }
            }
        }

        public string BucketName { get; }
        
        internal string RawKeySubject(string key)
        {
            return RawKeyPrefix + key;
        }
        
        internal string PubSubKeySubject(string key)
        {
            return PubSubKeyPrefix + key;
        }

        public KeyValueEntry Get(string key)
        {
            return _kvGetLastMessage(Validator.ValidateNonWildcardKvKeyRequired(key));
        }

        public KeyValueEntry Get(string key, ulong revision)
        {
            return _kvGetMessage(Validator.ValidateNonWildcardKvKeyRequired(key), revision);
        }

        internal KeyValueEntry _kvGetLastMessage(string key)
        {
            try {
                return new KeyValueEntry(jsm.GetLastMessage(StreamName, RawKeySubject(key)));
            }
            catch (NATSJetStreamException njse) {
                if (njse.ApiErrorCode == JetStreamConstants.JsNoMessageFoundErr) {
                    return null;
                }
                throw;
            }
        }

        internal KeyValueEntry _kvGetMessage(string key, ulong revision)
        {
            try {
                KeyValueEntry kve = new KeyValueEntry(jsm.GetMessage(StreamName, revision));
                return key.Equals(kve.Key) ? kve : null;
            }
            catch (NATSJetStreamException njse) {
                if (njse.ApiErrorCode == JetStreamConstants.JsNoMessageFoundErr) {
                    return null;
                }
                throw;
            }
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
                    KeyValueEntry kve = _kvGetLastMessage(key);
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
            return js.Publish(new Msg(PubSubKeySubject(key), h, data));
        }

        public IList<string> Keys()
        {
            IList<string> list = new List<string>();
            VisitSubject(RawKeySubject(">"), DeliverPolicy.LastPerSubject, true, false, m => {
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
            VisitSubject(RawKeySubject(key), DeliverPolicy.All, false, true, m => {
                list.Add(new KeyValueEntry(m));
            });
            return list;
        }

        public void PurgeDeletes() => PurgeDeletes(null);

        public void PurgeDeletes(KeyValuePurgeOptions options)
        {
            long dmThresh = options == null
                ? KeyValuePurgeOptions.DefaultThresholdMillis
                : options.DeleteMarkersThresholdMillis;

            DateTime limit;
            if (dmThresh < 0) {
                limit = DateTime.UtcNow.AddMilliseconds(600000); // long enough in the future to clear all
            }
            else if (dmThresh == 0) {
                limit = DateTime.UtcNow.AddMilliseconds(KeyValuePurgeOptions.DefaultThresholdMillis);
            }
            else {
                limit = DateTime.UtcNow.AddMilliseconds(-dmThresh);
            }

            IList<string> noKeepList = new List<string>();
            IList<string> keepList = new List<string>();
            VisitSubject(KeyValueUtil.ToStreamSubject(BucketName), DeliverPolicy.LastPerSubject, true, false, m =>
            {
                KeyValueEntry kve = new KeyValueEntry(m);
                if (!kve.Operation.Equals(KeyValueOperation.Put)) {
                    if (kve.Created > limit) // created > limit, so created after
                    {
                        keepList.Add(new BucketAndKey(m).Key);
                    }
                    else
                    {
                        noKeepList.Add(new BucketAndKey(m).Key);
                    }
                }
            });

            foreach (string key in noKeepList)
            {
                jsm.PurgeStream(StreamName, PurgeOptions.WithSubject(RawKeySubject(key)));
            }

            foreach (string key in keepList)
            {
                PurgeOptions po = PurgeOptions.Builder()
                    .WithSubject(RawKeySubject(key))
                    .WithKeep(1)
                    .Build();
                jsm.PurgeStream(StreamName, po);
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
                bool lastTimedOut = false;
                ulong pending = sub.GetConsumerInformation().CalculatedPending;
                while (pending > 0) // no need to loop if nothing pending
                {
                    try
                    {
                        Msg m = sub.NextMessage(js.Timeout);
                        action.Invoke(m);
                        if (--pending == 0)
                        {
                            return;
                        }
                        lastTimedOut = false;
                    }
                    catch (NATSTimeoutException)
                    {
                        if (lastTimedOut)
                        {
                            return; // two timeouts in a row is enough
                        }
                        lastTimedOut = true;
                    }
                }
            }
            finally
            {
                sub.Unsubscribe();
            }
        }
    }
}