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

namespace NATS.Client.JetStream
{
    public class JetStreamPushSyncSubscription : SyncSubscription, IJetStreamPushSyncSubscription, IJetStreamSubscriptionInternal
    {
        protected JetStream _js;
        protected string _consumer;
        protected string _stream;
        protected string _deliver;

        internal JetStreamPushSyncSubscription(Connection conn, string subject, string queue) 
            : base(conn, subject, queue) {}

        void IJetStreamSubscriptionInternal.SetupJetStream(JetStream js, string consumer, string stream, string deliver) {
            _js = js;
            _consumer = consumer;
            _stream = stream;
            _deliver = deliver;
        }

        public string Consumer => _consumer;
        public string Stream => _stream;
        public string DeliverSubject => _deliver;

        public JetStream GetContext() => _js;

        public ConsumerInfo GetConsumerInformation()
        {
            return _js.LookupConsumerInfo(_stream, _consumer);
        }

        public bool IsPullMode()
        {
            return false;
        }
    }
}
