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

using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATS.Client.KeyValue
{
    internal class KeyValueManagement : IKeyValueManagement
    {
        private readonly JetStreamManagement jsm;

        internal KeyValueManagement(IConnection connection, KeyValueOptions kvo)
        {
            jsm = (JetStreamManagement)connection.CreateJetStreamManagementContext(kvo?.JSOptions);
        }
        
        public KeyValueStatus Create(KeyValueConfiguration config)
        {
            StreamConfiguration sc = config.BackingConfig;
            if (jsm.Conn.ServerInfo.IsOlderThanVersion("2.7.2"))
            {
                sc = StreamConfiguration.Builder(sc).WithDiscardPolicy(null).Build(); // null discard policy will use default
            }
            return new KeyValueStatus(jsm.AddStream(sc));
        }

        public IList<string> GetBucketNames()
        {
            IList<string> buckets = new List<string>();
            IList<string> names = jsm.GetStreamNames();
            foreach (string name in names) {
                if (name.StartsWith(KeyValueUtil.KvStreamPrefix)) {
                    buckets.Add(KeyValueUtil.ExtractBucketName(name));
                }
            }
            return buckets;
        }

        public KeyValueStatus GetBucketInfo(string bucketName)
        {
            Validator.ValidateKvBucketNameRequired(bucketName);
            return new KeyValueStatus(jsm.GetStreamInfo(KeyValueUtil.ToStreamName(bucketName)));
        }

        public void Delete(string bucketName)
        {
            Validator.ValidateKvBucketNameRequired(bucketName);
            jsm.DeleteStream(KeyValueUtil.ToStreamName(bucketName));
        }
    }
}
