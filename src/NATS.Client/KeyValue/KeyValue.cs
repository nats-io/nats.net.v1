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
        private JetStream.JetStream js;
        private IJetStreamManagement jsm;
        private string stream;
        
        public KeyValue(IConnection connection, string bucketName) {
            BucketName = Validator.ValidateKvBucketNameRequired(bucketName);
            stream = KeyValueUtil.StreamName(BucketName);
            js = new JetStream.JetStream(connection, null);
            jsm = new JetStreamManagement(connection, null);
        }

        public string BucketName { get; }

        public KeyValueEntry Get(string key)
        {
            Validator.ValidateNonWildcardKvKeyRequired(key);
            string subj = string.Format(JetStreamConstants.JsapiMsgGet, stream);
            byte[] bytes = MessageGetRequest.LastBySubjectBytes(KeyValueUtil.KeySubject(BucketName, key));
            Msg resp = js.RequestResponseRequired(subj, bytes, JetStreamOptions.DefaultTimeout.Millis);
            MessageInfo mi = new MessageInfo(resp, false);
            if (mi.HasError) {
                if (mi.ApiErrorCode == JetStreamConstants.JsNoMessageFoundErr) {
                    return null; // run of the mill key not found
                }
                mi.ThrowOnHasError();
            }
            return new KeyValueEntry(mi);
        }

        public ulong Put(string key, byte[] value)
        {
            Validator.ValidateNonWildcardKvKeyRequired(key);
            PublishAck pa = js.Publish(new Msg(KeyValueUtil.KeySubject(BucketName, key), value));
            return pa.Seq;
        }

        public ulong Put(string key, string value) => Put(key, Encoding.UTF8.GetBytes(value));

        public ulong Put(string key, long value) => Put(key, Encoding.UTF8.GetBytes(value.ToString()));

        public void Delete(string key) => _deletePurge(key, KeyValueUtil.DeleteHeaders);

        public void Purge(string key) => _deletePurge(key, KeyValueUtil.PurgeHeaders);
        
        public KeyValueWatchSubscription Watch(string key, Action<KeyValueEntry> watcher, bool metaOnly, params KeyValueOperation[] operations)
        {
            Validator.ValidateKvKeyWildcardAllowedRequired(key);
            Validator.ValidateNotNull(watcher, "Watcher is required");
            return new KeyValueWatchSubscription(js, BucketName, key, metaOnly, watcher, operations);
        }

        public KeyValueWatchSubscription WatchAll(Action<KeyValueEntry> watcher, bool metaOnly, params KeyValueOperation[] operations)
        {
            Validator.ValidateNotNull(watcher, "Watcher is required");
            return new KeyValueWatchSubscription(js, BucketName, ">", metaOnly, watcher, operations);
        }

        private void _deletePurge(String key, MsgHeader h) {
            Validator.ValidateNonWildcardKvKeyRequired(key);
            js.Publish(new Msg(KeyValueUtil.KeySubject(BucketName, key), h, null));
        }

        public IList<string> Keys()
        {
            IList<String> list = new List<string>();
            VisitSubject(KeyValueUtil.StreamSubject(BucketName), DeliverPolicy.LastPerSubject, true, false, m => {
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
            VisitSubject(KeyValueUtil.KeySubject(BucketName, key), DeliverPolicy.All, false, true, m => {
                list.Add(new KeyValueEntry(m));
            });
            return list;
        }

        public void PurgeDeletes()
        {
            IList<String> list = new List<string>();
            VisitSubject(KeyValueUtil.StreamSubject(BucketName), DeliverPolicy.LastPerSubject, true, false, m => {
                KeyValueOperation op = KeyValueUtil.GetOperation(m.Header, KeyValueOperation.Put);
                if (!op.Equals(KeyValueOperation.Put)) {
                    list.Add(new BucketAndKey(m).Key);
                }
            });

            foreach (string key in list)
            {
                jsm.PurgeStream(stream, PurgeOptions.WithSubject(KeyValueUtil.KeySubject(BucketName, key)));
            }
        }

        public KeyValueStatus Status()
        {
            return new KeyValueStatus(jsm.GetStreamInfo(KeyValueUtil.StreamName(BucketName)));
        }
 
        private void VisitSubject(string subject, DeliverPolicy deliverPolicy, bool headersOnly, bool ordered, Action<Msg> action) {
            PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                // .WithOrdered(ordered) // TODO when available
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
                int timeout = 5000; // give plenty of time for the first
                while (true)
                {
                    Msg m = sub.NextMessage(timeout);
                    action.Invoke(m);
                    timeout = 100; // the rest should come pretty quick
                }
            }
            catch (NATSTimeoutException) { /* no more messages */ }
            sub.Unsubscribe();
        }
    }
}