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
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using static NATS.Client.JetStream.JetStreamBase;
using static NATS.Client.KeyValue.KeyValueUtil;

namespace NATS.Client.KeyValue
{
    internal class KeyValueManagementAsync : IKeyValueManagementAsync
    {
        private readonly JetStreamManagementAsync jsm;

        internal KeyValueManagementAsync(IConnection connection, KeyValueOptions kvo)
        {
            jsm = (JetStreamManagementAsync)connection.CreateJetStreamManagementAsyncContext(kvo?.JSOptions);
        }

        public async Task<KeyValueStatus> CreateAsync(
            KeyValueConfiguration config, 
            CancellationToken cancellationToken = default)
        {
            StreamConfiguration sc = config.BackingConfig;

            // most validation / KVC setup is done in the KeyValueConfiguration Builder
            // but this is done here because the context has a connection which has the server info with a version
            if (ServerInfoOrException(jsm.Conn).IsOlderThanVersion("2.7.2"))
            {
                sc = StreamConfiguration.Builder(sc).WithDiscardPolicy(null).Build(); // null discard policy will use default
            }
            return new KeyValueStatus(await jsm.AddStreamAsync(sc, cancellationToken).ConfigureAwait(false));
        }

        public async Task<KeyValueStatus> UpdateAsync(
            KeyValueConfiguration config, 
            CancellationToken cancellationToken = default)
        {
            return new KeyValueStatus(await jsm.UpdateStreamAsync(config.BackingConfig, cancellationToken).ConfigureAwait(false));

        }

        public async Task<IList<string>> GetBucketNamesAsync(CancellationToken cancellationToken = default)
        {
            IList<string> buckets = new List<string>();
            IList<string> names = await jsm.GetStreamNamesAsync(cancellationToken).ConfigureAwait(false);
            foreach (string name in names)
            {
                if (name.StartsWith(KvStreamPrefix))
                {
                    buckets.Add(ExtractBucketName(name));
                }
            }
            return buckets;
        }

        public async Task<KeyValueStatus> GetStatusAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            Validator.ValidateBucketName(bucketName, true);
            return new KeyValueStatus(await jsm.GetStreamInfoAsync(ToStreamName(bucketName), cancellationToken).ConfigureAwait(false));
        }

        public async Task<IList<KeyValueStatus>> GetStatusesAsync(CancellationToken cancellationToken = default)
        {
            IList<string> bucketNames = await GetBucketNamesAsync(cancellationToken).ConfigureAwait(false);
            IList<KeyValueStatus> statuses = new List<KeyValueStatus>();
            foreach (string name in bucketNames)
            {
                statuses.Add(new KeyValueStatus(await jsm.GetStreamInfoAsync(ToStreamName(name), cancellationToken).ConfigureAwait(false)));
            }
            return statuses;
        }

        public async Task DeleteAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            Validator.ValidateBucketName(bucketName, true);
            await jsm.DeleteStreamAsync(ToStreamName(bucketName), cancellationToken).ConfigureAwait(false);
        }
    }
}
