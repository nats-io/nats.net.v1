// Copyright 2022 The NATS Authors
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
using System.IO;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using static NATS.Client.ObjectStore.ObjectStoreUtil;
using static NATS.Client.ClientExDetail;

namespace NATS.Client.ObjectStore
{
    public class ObjectStore : FeatureBase, IObjectStore
    {
        internal ObjectStoreOptions oso;
        internal string RawChunkPrefix { get; }
        internal string PubSubChunkPrefix { get; }
        internal string RawMetaPrefix { get; }
        internal string PubSubMetaPrefix { get; }

        public string BucketName { get; }
        
        internal string RawChunkSubject(string nuid) {
            return RawChunkPrefix + nuid;
        }

        internal string PubSubChunkSubject(string nuid) {
            return PubSubChunkPrefix + nuid;
        }

        internal string RawMetaSubject(string name) {
            return RawMetaPrefix + EncodeForSubject(name);
        }

        internal string RawAllMetaSubject()
        {
            return RawMetaPrefix + ">";
        }

        internal string PubSubMetaSubject(string name) {
            return PubSubMetaPrefix + EncodeForSubject(name);
        }

        private ObjectInfo PublishMeta(ObjectInfo info) {
            js.Publish(new Msg(PubSubMetaSubject(info.ObjectName), MetaHeaders, info.Serialize()));
            return ObjectInfo.Builder(info).WithModified(DateTime.UtcNow).Build();
        }

        internal ObjectStore(IConnection connection, string bucketName, ObjectStoreOptions oso) : base(connection, oso)
        {
            this.oso = oso;
            BucketName = Validator.ValidateBucketName(bucketName, true);
            StreamName = ToStreamName(bucketName);
            RawChunkPrefix = ToChunkPrefix(bucketName);
            RawMetaPrefix = ToMetaPrefix(bucketName);
            if (oso == null) {
                PubSubChunkPrefix = RawChunkPrefix;
                PubSubMetaPrefix = RawMetaPrefix;
            }
            else if (oso.JSOptions.IsDefaultPrefix) {
                PubSubChunkPrefix = RawChunkPrefix;
                PubSubMetaPrefix = RawMetaPrefix;
            }
            else {
                PubSubChunkPrefix = oso.JSOptions.Prefix + RawChunkPrefix;
                PubSubMetaPrefix = oso.JSOptions.Prefix + RawMetaPrefix;
            }
        }

        public ObjectInfo Put(ObjectMeta meta, Stream inputStream)
        {
            Validator.ValidateNotNull(meta, "ObjectMeta");
            Validator.ValidateNotNull(meta.ObjectName, "ObjectMeta name");
            Validator.ValidateNotNull(inputStream, "InputStream");
            if (meta.ObjectMetaOptions.Link != null) {
                throw OsLinkNotAllowOnPut.Instance();
            }

            string nuid = Nuid.NextGlobal();
            string chunkSubject = PubSubChunkSubject(nuid);

            int chunkSize = meta.ObjectMetaOptions.ChunkSize;
            if (chunkSize <= 0) {
                chunkSize = DefaultChunkSize;
            }

            try
            {
                Digester digester = new Digester();
                long totalSize = 0; // track total bytes read to make sure
                int chunks = 0;

                // working with chunkSize number of bytes each time.
                byte[] buffer = new byte[chunkSize];
                int red = chunkSize;
                while (red == chunkSize)
                {
                    // keep reading while last chunk was full size
                    red = inputStream.Read(buffer, 0, chunkSize);
                    if (red > 0)
                    {
                        // copy if red is less than chunk size
                        byte[] payload = buffer;
                        if (red < chunkSize)
                        {
                            payload = new byte[red];
                            Array.Copy(buffer, payload, red);
                        }

                        // digest the actual bytes
                        digester.AppendData(payload);

                        // publish the payload
                        js.Publish(chunkSubject, payload);

                        // track total chunks and bytes
                        chunks++;
                        totalSize += red;
                    }
                }

                return PublishMeta(ObjectInfo.Builder(BucketName, meta)
                    .WithSize(totalSize)
                    .WithChunks(chunks)
                    .WithNuid(nuid)
                    .WithChunkSize(chunkSize)
                    .WithDigest(digester.GetDigestEntry())
                    .Build());
            }
            catch (Exception)
            {
                try {
                    jsm.PurgeStream(StreamName, PurgeOptions.WithSubject(RawChunkSubject(nuid)));
                }
                catch (Exception) { /* ignore, there is already an error */ }
                
                throw;
            }
        }

        public ObjectInfo Put(string objectName, Stream inputStream)
        {
            return Put(ObjectMeta.ForObjectName(objectName), inputStream);
        }

        public ObjectInfo Put(string objectName, byte[] input)
        {
            return Put(objectName, new MemoryStream(input));
        }

        public ObjectInfo Put(FileInfo fileInfo)
        {
            return Put(ObjectMeta.ForObjectName(fileInfo.Name), fileInfo.OpenRead());
        }

        public ObjectInfo Get(string objectName, Stream outputStream)
        {
            ObjectInfo oi = GetInfo(objectName, false);
            if (oi == null) {
                throw OsObjectNotFound.Instance();
            }

            if (oi.IsLink) {
                ObjectLink link = oi.Link;
                if (link.IsBucketLink) {
                    throw OsGetLinkToBucket.Instance();
                }

                // is the link in the same bucket
                if (link.Bucket.Equals(BucketName)) {
                    return Get(link.ObjectName, outputStream);
                }

                // different bucket
                // get the store for the linked bucket, then get the linked object
                return js.Conn.CreateObjectStoreContext(link.Bucket, oso).Get(link.ObjectName, outputStream);
            }

            Digester digester = new Digester();
            long totalBytes = 0;
            long totalChunks = 0;

            // if there is one chunk, just go get the message directly and we're done.
            if (oi.Chunks == 1) {
                MessageInfo mi = jsm.GetLastMessage(StreamName, RawChunkSubject(oi.Nuid));
                // track the byte count and chunks
                // update the digest
                // write the bytes to the output file
                totalBytes = mi.Data.Length;
                totalChunks = 1;
                digester.AppendData(mi.Data);
                outputStream.Write(mi.Data, 0, mi.Data.Length);
            }
            else {
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(
                    PubSubChunkSubject(oi.Nuid),
                    PushSubscribeOptions.Builder()
                        .WithStream(StreamName)
                        .WithOrdered(true)
                        .Build());

                bool notFinished = true;
                while (notFinished) {
                    try
                    {
                        Msg m = sub.NextMessage(1000);
                        // track the byte count and chunks
                        // update the digest
                        // write the bytes to the output file
                        totalBytes += m.Data.Length;
                        totalChunks++;
                        digester.AppendData(m.Data);
                        outputStream.Write(m.Data, 0, m.Data.Length);
                    }
                    catch (NATSTimeoutException)
                    {
                        notFinished = false;
                    }
                }
                sub.Unsubscribe();
            }
            outputStream.Flush();

            if (totalBytes != oi.Size) { throw OsGetSizeMismatch.Instance(); }
            if (totalChunks != oi.Chunks) { throw OsGetChunksMismatch.Instance(); }
            if (!digester.DigestEntriesMatch(oi.Digest)) { throw OsGetDigestMismatch.Instance(); }

            return oi;
        }

        public ObjectInfo GetInfo(string objectName)
        {
            return GetInfo(objectName, false);
        }

        public ObjectInfo GetInfo(string objectName, bool includingDeleted)
        {
            MessageInfo mi = _getLast(RawMetaSubject(objectName));
            if (mi == null)
            {
                return null;
            }
            ObjectInfo info = new ObjectInfo(mi);
            return includingDeleted || !info.IsDeleted ? info : null;
        }

        public ObjectInfo UpdateMeta(string objectName, ObjectMeta meta)
        {
            Validator.ValidateNotNull(objectName, "object name");
            Validator.ValidateNotNull(meta, "ObjectMeta");
            Validator.ValidateNotNull(meta.ObjectName, "ObjectMeta name");

            ObjectInfo currentInfo = GetInfo(objectName, true);
            if (currentInfo == null) {
                throw OsObjectNotFound.Instance();
            }
            if (currentInfo.IsDeleted) {
                throw OsObjectIsDeleted.Instance();
            }

            bool nameChange = !objectName.Equals(meta.ObjectName);
            if (nameChange) {
                if (GetInfo(meta.ObjectName, false) != null) {
                    throw OsObjectAlreadyExists.Instance();
                }
            }

            currentInfo = PublishMeta(ObjectInfo.Builder(currentInfo)
                .WithObjectName(meta.ObjectName)   // replace the name
                .WithDescription(meta.Description) // replace the description
                .WithHeaders(meta.Headers)         // replace the headers
                .Build());

            if (nameChange) {
                // delete the meta from the old name via purge stream for subject
                jsm.PurgeStream(StreamName, PurgeOptions.WithSubject(RawMetaSubject(objectName)));
            }

            return currentInfo;
        }

        public ObjectInfo Delete(string objectName)
        {
            ObjectInfo info = GetInfo(objectName, true);
            if (info == null) {
                throw OsObjectNotFound.Instance();
            }

            if (info.IsDeleted) {
                return info;
            }

            ObjectInfo deleted = PublishMeta(ObjectInfo.Builder(info)
                .WithDeleted(true)
                .WithSize(0)
                .WithChunks(0)
                .WithDigest(null)
                .Build());

            jsm.PurgeStream(StreamName, PurgeOptions.WithSubject(RawChunkSubject(info.Nuid)));
            return deleted;
        }

        public ObjectInfo AddLink(string objectName, ObjectInfo toInfo)
        {
            Validator.ValidateNotNull(objectName, "object name");
            Validator.ValidateNotNull(toInfo, "Link-To ObjectInfo");
            Validator.ValidateNotNull(toInfo.ObjectName, "Link-To ObjectMeta");

            if (toInfo.IsDeleted) {
                throw OsObjectIsDeleted.Instance();
            }

            if (toInfo.IsLink) {
                throw OsCantLinkToLink.Instance();
            }

            ObjectInfo info = GetInfo(objectName, false);
            if (info != null && !info.IsLink) {
                throw OsObjectAlreadyExists.Instance();
            }

            return PublishMeta(ObjectInfo.Builder(BucketName, objectName)
                .WithNuid(Nuid.NextGlobal())
                .WithObjectLink(toInfo.Bucket, toInfo.ObjectName)
                .Build());
        }

        public ObjectInfo AddBucketLink(string objectName, IObjectStore toStore)
        {
            Validator.ValidateNotNull(objectName, "object name");
            Validator.ValidateNotNull(toStore, "Link-To ObjectStore");

            ObjectInfo info = GetInfo(objectName, false);
            if (info != null && !info.IsLink) {
                throw OsObjectAlreadyExists.Instance();
            }

            return PublishMeta(ObjectInfo.Builder(BucketName, objectName)
                .WithNuid(Nuid.NextGlobal())
                .WithBucketLink(toStore.BucketName)
                .Build());
        }

        public ObjectStoreStatus Seal()
        {
            StreamInfo si = jsm.GetStreamInfo(StreamName);
            si = jsm.UpdateStream(StreamConfiguration.Builder(si.Config).Seal().Build());
            return new ObjectStoreStatus(si);
        }

        public IList<ObjectInfo> GetList()
        {
            IList<ObjectInfo> list = new List<ObjectInfo>();
            VisitSubject(RawAllMetaSubject(), DeliverPolicy.LastPerSubject, false, true, m => {
                ObjectInfo oi = new ObjectInfo(m);
                if (!oi.IsDeleted) {
                    list.Add(oi);
                }
            });
            return list;
        }

        public ObjectStoreWatchSubscription Watch(IObjectStoreWatcher watcher, params ObjectStoreWatchOption[] watchOptions)
        {
            return new ObjectStoreWatchSubscription(this, watcher, watchOptions);
        }

        public ObjectStoreStatus GetStatus()
        {
            return new ObjectStoreStatus(jsm.GetStreamInfo(StreamName));
        }
    }
}
