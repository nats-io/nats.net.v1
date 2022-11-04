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
using static NATS.Client.Internals.NatsConstants;
using static NATS.Client.KeyValue.KeyValueUtil;

namespace NATS.Client.KeyValue
{
    public class KeyValue : FeatureBase, IKeyValue
    {
        internal string StreamSubject { get; }
        internal string ReadPrefix { get; }
        internal string WritePrefix { get; }
        
        internal KeyValue(IConnection connection, string bucketName, KeyValueOptions kvo) : base(connection, kvo) {
            BucketName = Validator.ValidateBucketName(bucketName, true);
            StreamName = ToStreamName(BucketName);

            StreamInfo si = jsm.GetStreamInfo(StreamName);

            StreamSubject = ToStreamSubject(BucketName);
            ReadPrefix = ToKeyPrefix(bucketName);

            Mirror m = si.Config.Mirror;
            if (m != null)
            {
                string bName = TrimPrefix(m.Name);
                if (m.External?.Api == null)
                {
                    WritePrefix = ToKeyPrefix(bName);
                }
                else
                {
                    ReadPrefix = ToKeyPrefix(bName);
                    WritePrefix = m.External.Api + Dot + ToKeyPrefix(bName);
                }
            }
            else if (kvo == null || kvo.JSOptions.IsDefaultPrefix)
            {
                WritePrefix = ReadPrefix;
            }
            else
            {
                WritePrefix = kvo.JSOptions.Prefix + ReadPrefix;
            }
        }

        public string BucketName { get; }
        
        internal string ReadSubject(string key)
        {
            return ReadPrefix + key;
        }
        
        internal string WriteSubject(string key)
        {
            return WritePrefix + key;
        }

        public KeyValueEntry Get(string key)
        {
            return existingOnly(_get(Validator.ValidateNonWildcardKvKeyRequired(key)));
        }

        public KeyValueEntry Get(string key, ulong revision)
        {
            return existingOnly(_getBySeq(Validator.ValidateNonWildcardKvKeyRequired(key), revision));
        }

        private KeyValueEntry existingOnly(KeyValueEntry kve) {
            return kve == null || !kve.Operation.Equals(KeyValueOperation.Put) ? null : kve;
        }
        
        KeyValueEntry _get(string key) {
            MessageInfo mi = _getLast(ReadSubject(key));
            return mi == null ? null : new KeyValueEntry(mi);
        }

        KeyValueEntry _getBySeq(string key, ulong revision)
        {
            MessageInfo mi = _getBySeq(revision);
            if (mi != null)
            {
                KeyValueEntry kve = new KeyValueEntry(mi);
                if (key.Equals(kve.Key)) {
                    return kve;
                }
            }

            return null;
        }

        public ulong Put(string key, byte[] value)
        {
            return _write(key, value, null).Seq;
        }

        public ulong Put(string key, string value) => Put(key, Encoding.UTF8.GetBytes(value));

        public ulong Put(string key, long value) => Put(key, Encoding.UTF8.GetBytes(value.ToString()));
        
        public ulong Create(string key, byte[] value)
        {
            Validator.ValidateNonWildcardKvKeyRequired(key);
            try
            {
                return Update(key, value, 0);
            }
            catch (NATSJetStreamException e)
            {
                if (e.ApiErrorCode == JetStreamConstants.JsWrongLastSequence)
                {
                    // must check if the last message for this subject is a delete or purge
                    KeyValueEntry kve = _get(key);
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
            return _write(key, value, h).Seq;
        }

        public void Delete(string key) => _write(key, null, DeleteHeaders);

        public void Purge(string key) => _write(key, null, PurgeHeaders);
        
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

        private PublishAck _write(string key, byte[] data, MsgHeader h) {
            Validator.ValidateNonWildcardKvKeyRequired(key);
            return js.Publish(new Msg(WriteSubject(key), h, data));
        }

        public IList<string> Keys()
        {
            IList<string> list = new List<string>();
            VisitSubject(ReadSubject(">"), DeliverPolicy.LastPerSubject, true, false, m => {
                KeyValueOperation op = GetOperation(m.Header, KeyValueOperation.Put);
                if (op.Equals(KeyValueOperation.Put)) {
                    list.Add(new BucketAndKey(m).Key);
                }
            });
            return list;
        }

        public IList<KeyValueEntry> History(string key)
        {
            IList<KeyValueEntry> list = new List<KeyValueEntry>();
            VisitSubject(ReadSubject(key), DeliverPolicy.All, false, true, m => {
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
            VisitSubject(ToStreamSubject(BucketName), DeliverPolicy.LastPerSubject, true, false, m =>
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
                jsm.PurgeStream(StreamName, PurgeOptions.WithSubject(ReadSubject(key)));
            }

            foreach (string key in keepList)
            {
                PurgeOptions po = PurgeOptions.Builder()
                    .WithSubject(ReadSubject(key))
                    .WithKeep(1)
                    .Build();
                jsm.PurgeStream(StreamName, po);
            }
        }

        public KeyValueStatus Status()
        {
            return new KeyValueStatus(jsm.GetStreamInfo(ToStreamName(BucketName)));
        }
    }
}