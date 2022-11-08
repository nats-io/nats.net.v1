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
using static NATS.Client.JetStream.JetStreamBase;
using static NATS.Client.KeyValue.KeyValueUtil;

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

            // most validation / KVC setup is done in the KeyValueConfiguration Builder
            // but this is done here because the context has a connection which has the server info with a version
            if (ServerInfoOrException(jsm.Conn).IsOlderThanVersion("2.7.2"))
            {
                sc = StreamConfiguration.Builder(sc).WithDiscardPolicy(null).Build(); // null discard policy will use default
            }
            return new KeyValueStatus(jsm.AddStream(sc));
        }
        
        public KeyValueStatus Update(KeyValueConfiguration config)
        {
            return new KeyValueStatus(jsm.UpdateStream(config.BackingConfig));
        }

        public IList<string> GetBucketNames()
        {
            IList<string> buckets = new List<string>();
            IList<string> names = jsm.GetStreamNames();
            foreach (string name in names) {
                if (name.StartsWith(KvStreamPrefix)) {
                    buckets.Add(ExtractBucketName(name));
                }
            }
            return buckets;
        }

        public KeyValueStatus GetBucketInfo(string bucketName) => GetStatus(bucketName);

        public KeyValueStatus GetStatus(string bucketName)
        {
            Validator.ValidateBucketName(bucketName, true);
            return new KeyValueStatus(jsm.GetStreamInfo(ToStreamName(bucketName)));
        }

        public IList<KeyValueStatus> GetStatuses()
        {
            IList<string> bucketNames = GetBucketNames();
            IList<KeyValueStatus> statuses = new List<KeyValueStatus>();
            foreach (string name in bucketNames) {
                statuses.Add(new KeyValueStatus(jsm.GetStreamInfo(ToStreamName(name))));
            }
            return statuses;
        }

        public void Delete(string bucketName)
        {
            Validator.ValidateBucketName(bucketName, true);
            jsm.DeleteStream(ToStreamName(bucketName));
        }
    }
}
