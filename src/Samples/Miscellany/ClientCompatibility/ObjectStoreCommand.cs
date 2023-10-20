// Copyright 2023 The NATS Authors
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
using System.IO;
using System.Net;
using System.Threading;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using NATS.Client.ObjectStore;
using static NATSExamples.ClientCompatibility.Test;

namespace NATSExamples.ClientCompatibility
{
    public class ObjectStoreCommand : Command
    {
        readonly string bucket;
        readonly string obj;
        readonly string url;
    
        public ObjectStoreCommand(IConnection c, TestMessage tm) : base(c, tm) {
            bucket = full["bucket"];
            obj = full["object"];
            url = full["url"];
        }
    
        public void execute() {
            switch (test) {
                case DefaultBucket:  doCreateBucket(); break;
                case CustomBucket:   doCreateBucket(); break;
                case GetObject:      doGetObject(); break;
                case PutObject:      doPutObject(); break;
                case GetLink:        doGetLink(); break;
                case PutLink:        doPutLink(); break;
                case UpdateMetadata: doUpdateMetadata(); break;
                case Watch:          doWatch(); break;
                case WatchUpdate:    doWatchUpdate(); break;
                default:
                    respond();
                    break;
            }
        }
    
        private void doCreateBucket() {
            try {
                createBucket();
                respond();
            }
            catch (Exception e) {
                handleException(e);
            }
        }
    
        private void doPutObject() {
            try {
                string objectName = config["name"];
                string description = config["description"];
                IObjectStore os = c.CreateObjectStoreContext(bucket);
                ObjectMeta meta = ObjectMeta.Builder(objectName).WithDescription(description).Build();
                using (var client = new WebClient())
                {
                    var content = client.DownloadData(url);
                    using (var stream = new MemoryStream(content))
                    {
                        os.Put(meta, stream);
                    }
                } 
                respond();
            }
            catch (Exception e) {
                handleException(e);
            }
        }
    
        private void doGetObject() {
            try {
                IObjectStore os = c.CreateObjectStoreContext(bucket);
                MemoryStream stream = new MemoryStream();
                respondDigest(os.Get(obj, stream));
            }
            catch (Exception e) {
                handleException(e);
            }
        }
    
        private void doUpdateMetadata() {
            try {
                IObjectStore os = c.CreateObjectStoreContext(bucket);
                string objectName = config["name"];
                string description = config["description"];
                ObjectMeta meta = ObjectMeta.Builder(objectName).WithDescription(description).Build();
                os.UpdateMeta(obj, meta);
                respond();
            }
            catch (Exception e) {
                handleException(e);
            }
        }
    
        private void doGetLink() {
            try {
                IObjectStore os = c.CreateObjectStoreContext(bucket);
                MemoryStream stream = new MemoryStream();
                respondDigest(os.Get(obj, stream));
            }
            catch (Exception e) {
                handleException(e);
            }
        }
    
        private void doPutLink() {
            try {
                string linkName = full["link_name"];
                IObjectStore os = c.CreateObjectStoreContext(bucket);
                MemoryStream stream = new MemoryStream();
                ObjectInfo oi = os.Get(obj, stream);
                os.AddLink(linkName, oi);
                respond();
            }
            catch (Exception e) {
                handleException(e);
            }
        }
    
        private void doWatch() {
            try {
                IObjectStore os = c.CreateObjectStoreContext(bucket);
                WatchWatcher ww = new WatchWatcher();
                ObjectStoreWatchSubscription ws = os.Watch(ww);
                ww.latch.Wait(TimeSpan.FromSeconds(2));
                respond(ww.result);
            }
            catch (Exception e) {
                handleException(e);
            }
        }
    
        class WatchWatcher : IObjectStoreWatcher
        {
            public CountdownEvent latch = new CountdownEvent(1);
            public string result;
    
            public void Watch(ObjectInfo oi) {
                if (result == null) {
                    result = oi.Digest;
                }
                else {
                    result = result + "," + oi.Digest;
                    latch.Signal();
                }
            }
    
            public void EndOfData() {}
        }
    
        private void doWatchUpdate() {
            try {
                IObjectStore os = c.CreateObjectStoreContext(bucket);
                WatchUpdatesWatcher wuw = new WatchUpdatesWatcher();
                ObjectStoreWatchSubscription ws = os.Watch(wuw);
                wuw.latch.Wait(TimeSpan.FromSeconds(2));
                respond(wuw.result);
            }
            catch (Exception e) {
                handleException(e);
            }
        }
    
        class WatchUpdatesWatcher : IObjectStoreWatcher
        {
            public CountdownEvent latch = new CountdownEvent(1);
            public string result;
    
            public void Watch(ObjectInfo oi) {
                if (result == null) {
                    result = oi.Digest;
                }
                else {
                    result = oi.Digest;
                    latch.Signal();
                }
            }
    
            public void EndOfData() {}
        }
    
        protected void respondDigest(ObjectInfo oi) {
            Log.info($"RESPOND {subject} digest {oi.Digest}");
            byte[] payload = Convert.FromBase64String(oi.Digest.Substring(8).Replace('-', '+').Replace('_', '/')); 
            c.Publish(replyTo, payload);
        }
    
        private void createBucket() {
            IObjectStoreManagement osm;
            try {
                osm = c.CreateObjectStoreManagementContext();
            }
            catch (IOException e) {
                handleException(e);
                return;
            }
    
            ObjectStoreConfiguration osc = extractObjectStoreConfiguration();
            try {
                osm.Delete(osc.BucketName);
            }
            catch (Exception) {}
    
            try {
                osm.Create(osc);
            }
            catch (Exception e) {
                handleException(e);
            }
        }
    
        private ObjectStoreConfiguration extractObjectStoreConfiguration() {
            ObjectStoreConfiguration.ObjectStoreConfigurationBuilder builder = ObjectStoreConfiguration.Builder();
            if (bucket != null) {
                builder.WithName(bucket);
            }
            else {
                string s = config[ApiConstants.Bucket];
                if (s != null) {
                    builder.WithName(s);
                }
                s = config[ApiConstants.Description];
                if (s != null) {
                    builder.WithDescription(s);
                }
                long l = JsonUtils.AsLongOrMinus1(config, ApiConstants.MaxBytes);
                if (l != -1) {
                    builder.WithMaxBucketSize(l);
                }
                Duration d = JsonUtils.AsDuration(config, ApiConstants.MaxAge, null);
                if (d != null) {
                    builder.WithTtl(d);
                }
                s = config[ApiConstants.Storage];
                if (s != null)
                {
                    StorageType st = ApiEnums.GetValueOrDefault(s, StorageType.File);
                    if (st == StorageType.Memory)
                    {
                        builder.WithStorageType(StorageType.Memory);
                    }
                }
                
                int i = JsonUtils.AsIntOrMinus1(config, ApiConstants.NumReplicas);
                if (i != -1) {
                    builder.WithReplicas(i);
                }
            }
            return builder.Build();
        }
    }
}
