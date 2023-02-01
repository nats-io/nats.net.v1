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
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using static NATS.Client.Internals.NatsConstants;
using static NATS.Client.KeyValue.KeyValueUtil;

namespace NATS.Client.KeyValue
{
    public class KeyValueAsync : FeatureBaseAsync, IKeyValueAsync
    {
        private readonly KeyValueOptions keyValueOptions;
        internal string StreamSubject { get; }
        internal string ReadPrefix { get; private set; }
        internal string WritePrefix { get; private set; }

        internal KeyValueAsync(IConnection connection, string bucketName, KeyValueOptions kvo) : base(connection, kvo) {
            this.keyValueOptions = kvo;
            BucketName = Validator.ValidateBucketName(bucketName, true);
            StreamName = ToStreamName(BucketName);
            StreamSubject = ToStreamSubject(BucketName);
        }

        private async Task EnsureWritePrefixAsync(CancellationToken cancellationToken)
        {
            StreamInfo si = await jsm.GetStreamInfoAsync(StreamName, cancellationToken).ConfigureAwait(false);
            ReadPrefix = ToKeyPrefix(BucketName);

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
            else if (keyValueOptions == null || keyValueOptions.JSOptions.IsDefaultPrefix)
            {
                WritePrefix = ReadPrefix;
            }
            else
            {
                WritePrefix = keyValueOptions.JSOptions.Prefix + ReadPrefix;
            }

        }

        public string BucketName { get; }

        internal async Task<string> ReadSubject(string key, CancellationToken cancellationToken = default)
        {
            await EnsureWritePrefixAsync(cancellationToken).ConfigureAwait(false);
            return ReadPrefix + key;
        }

        internal async Task<string> WriteSubject(string key, CancellationToken cancellationToken = default)
        {
            await EnsureWritePrefixAsync(cancellationToken).ConfigureAwait(false);
            return WritePrefix + key;
        }

        public async Task<KeyValueEntry> GetAsync(string key, CancellationToken cancellationToken = default)
        {
            return existingOnly(await _getAsync(Validator.ValidateNonWildcardKvKeyRequired(key), cancellationToken).ConfigureAwait(false));
        }

        public async Task<KeyValueEntry> GetAsync(string key, ulong revision, CancellationToken cancellationToken = default)
        {
            return existingOnly(await _getBySeqAsync(Validator.ValidateNonWildcardKvKeyRequired(key), revision, cancellationToken).ConfigureAwait(false));
        }

        private KeyValueEntry existingOnly(KeyValueEntry kve) {
            return kve == null || !kve.Operation.Equals(KeyValueOperation.Put) ? null : kve;
        }
        
        async Task<KeyValueEntry> _getAsync(string key, CancellationToken cancellationToken) {
            MessageInfo mi = await _getLastAsync(await ReadSubject(key, cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
            return mi == null ? null : new KeyValueEntry(mi);
        }

        async Task<KeyValueEntry> _getBySeqAsync(string key, ulong revision, CancellationToken cancellationToken)
        {
            MessageInfo mi = await _getBySeqAsync(revision, cancellationToken).ConfigureAwait(false);
            if (mi != null)
            {
                KeyValueEntry kve = new KeyValueEntry(mi);
                if (key.Equals(kve.Key)) {
                    return kve;
                }
            }

            return null;
        }

        public async Task<ulong> PutAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            return (await _writeAsync(key, value, null, cancellationToken).ConfigureAwait(false)).Seq;
        }

        public async Task<ulong> PutAsync(string key, string value, CancellationToken cancellationToken = default) =>
            await PutAsync(key, Encoding.UTF8.GetBytes(value), cancellationToken).ConfigureAwait(false);

        public async Task<ulong> PutAsync(string key, long value, CancellationToken cancellationToken = default) =>
            await PutAsync(key, Encoding.UTF8.GetBytes(value.ToString()), cancellationToken).ConfigureAwait(false);

        public async Task<ulong> CreateAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            Validator.ValidateNonWildcardKvKeyRequired(key);
            try
            {
                return await UpdateAsync(key, value, 0, cancellationToken).ConfigureAwait(false);
            }
            catch (NATSJetStreamException e)
            {
                if (e.ApiErrorCode == JetStreamConstants.JsWrongLastSequence)
                {
                    // must check if the last message for this subject is a delete or purge
                    KeyValueEntry kve = await _getAsync(key, cancellationToken).ConfigureAwait(false);
                    if (kve != null && !kve.Operation.Equals(KeyValueOperation.Put)) {
                        return await UpdateAsync(key, value, kve.Revision, cancellationToken).ConfigureAwait(false);
                    }
                }

                throw;
            }
        }

        public async Task<ulong> UpdateAsync(string key, byte[] value, ulong expectedRevision, CancellationToken cancellationToken = default)
        {
            MsgHeader h = new MsgHeader 
            {
                [JetStreamConstants.ExpLastSubjectSeqHeader] = expectedRevision.ToString()
            };
            return (await _writeAsync(key, value, h, cancellationToken).ConfigureAwait(false)).Seq;
        }

        public async Task DeleteAsync(string key, CancellationToken cancellationToken = default) =>
            await _writeAsync(key, null, DeleteHeaders, cancellationToken).ConfigureAwait(false);

        public async Task PurgeAsync(string key, CancellationToken cancellationToken = default) =>
            await _writeAsync(key, null, PurgeHeaders, cancellationToken).ConfigureAwait(false);

        //public KeyValueWatchSubscription Watch(string key, IKeyValueWatcher watcher, params KeyValueWatchOption[] watchOptions)
        //{
        //    Validator.ValidateKvKeyWildcardAllowedRequired(key);
        //    Validator.ValidateNotNull(watcher, "Watcher is required");
        //    return new KeyValueWatchSubscription(this, key, watcher, watchOptions);
        //}

        //public KeyValueWatchSubscription WatchAll(IKeyValueWatcher watcher, params KeyValueWatchOption[] watchOptions)
        //{
        //    Validator.ValidateNotNull(watcher, "Watcher is required");
        //    return new KeyValueWatchSubscription(this, ">", watcher, watchOptions);
        //}

        private async Task<PublishAck> _writeAsync(string key, byte[] data, MsgHeader h, CancellationToken cancellationToken) {
            Validator.ValidateNonWildcardKvKeyRequired(key);
            return await js.PublishAsync(new Msg(await WriteSubject(key, cancellationToken).ConfigureAwait(false), h, data), cancellationToken).ConfigureAwait(false);
        }

        //public async Task<IList<string>> KeysAsync(CancellationToken cancellationToken = default)
        //{
        //    IList<string> list = new List<string>();
        //    VisitSubject(ReadSubject(">"), DeliverPolicy.LastPerSubject, true, false, m => {
        //        KeyValueOperation op = GetOperation(m.Header, KeyValueOperation.Put);
        //        if (op.Equals(KeyValueOperation.Put)) {
        //            list.Add(new BucketAndKey(m).Key);
        //        }
        //    });
        //    return list;
        //}

        //public IList<KeyValueEntry> History(string key)
        //{
        //    IList<KeyValueEntry> list = new List<KeyValueEntry>();
        //    VisitSubject(ReadSubject(key), DeliverPolicy.All, false, true, m => {
        //        list.Add(new KeyValueEntry(m));
        //    });
        //    return list;
        //}

        //public void PurgeDeletes() => PurgeDeletes(null);

        //public void PurgeDeletes(KeyValuePurgeOptions options)
        //{
        //    long dmThresh = options == null
        //        ? KeyValuePurgeOptions.DefaultThresholdMillis
        //        : options.DeleteMarkersThresholdMillis;

        //    DateTime limit;
        //    if (dmThresh < 0) {
        //        limit = DateTime.UtcNow.AddMilliseconds(600000); // long enough in the future to clear all
        //    }
        //    else if (dmThresh == 0) {
        //        limit = DateTime.UtcNow.AddMilliseconds(KeyValuePurgeOptions.DefaultThresholdMillis);
        //    }
        //    else {
        //        limit = DateTime.UtcNow.AddMilliseconds(-dmThresh);
        //    }

        //    IList<string> noKeepList = new List<string>();
        //    IList<string> keepList = new List<string>();
        //    VisitSubject(ToStreamSubject(BucketName), DeliverPolicy.LastPerSubject, true, false, m =>
        //    {
        //        KeyValueEntry kve = new KeyValueEntry(m);
        //        if (!kve.Operation.Equals(KeyValueOperation.Put)) {
        //            if (kve.Created > limit) // created > limit, so created after
        //            {
        //                keepList.Add(new BucketAndKey(m).Key);
        //            }
        //            else
        //            {
        //                noKeepList.Add(new BucketAndKey(m).Key);
        //            }
        //        }
        //    });

        //    foreach (string key in noKeepList)
        //    {
        //        jsm.PurgeStream(StreamName, PurgeOptions.WithSubject(ReadSubject(key)));
        //    }

        //    foreach (string key in keepList)
        //    {
        //        PurgeOptions po = PurgeOptions.Builder()
        //            .WithSubject(ReadSubject(key))
        //            .WithKeep(1)
        //            .Build();
        //        jsm.PurgeStream(StreamName, po);
        //    }
        //}

        public async Task<KeyValueStatus> StatusAsync(CancellationToken cancellationToken = default)
        {
            return new KeyValueStatus(await jsm.GetStreamInfoAsync(ToStreamName(BucketName), cancellationToken).ConfigureAwait(false));
        }
    }
}