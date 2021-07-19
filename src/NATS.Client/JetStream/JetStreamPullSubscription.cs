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

using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public class JetStreamPullSubscription : SyncSubscription, IJetStreamPullSubscription, IJetStreamSubscriptionInternal
    {
        protected JetStream _js;
        protected string _consumer;
        protected string _stream;
        protected string _deliver;

        internal JetStreamPullSubscription(Connection conn, string subject, string queue)
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
            return true;
        }

        public void Pull(int batchSize)
        {
            PullInternal(batchSize, false, null);
        }

        public void PullExpiresIn(int batchSize, Duration expiresIn)
        {
            PullInternal(batchSize, false, expiresIn);
        }

        public void PullExpiresIn(int batchSize, int expiresInMillis)
        {
            PullInternal(batchSize, false, Duration.OfMillis(expiresInMillis));
        }

        public void PullNoWait(int batchSize)
        {
            PullInternal(batchSize, true, null);
        }

        private void PullInternal(int batchSize, bool noWait, Duration expiresIn) {
            int batch = Validator.ValidatePullBatchSize(batchSize);
            string subj = string.Format(JetStreamConstants.JsapiConsumerMsgNext, Stream, Consumer);
            string publishSubject = GetContext().PrependPrefix(subj);
            Connection.Publish(publishSubject, Subject, GetPullJson(batch, noWait, expiresIn));
            Connection.FlushBuffer();
        }

        private byte[] GetPullJson(int batch, bool noWait, Duration expiresIn)
        {
            JSONObject jso = new JSONObject {["batch"] = batch};
            if (noWait)
            {
                jso["no_wait"] = true;
            }
            if (expiresIn != null && expiresIn.IsPositive())
            {
                jso["expires"] = expiresIn.Nanos;
            }

            return JsonUtils.Serialize(jso);
        }

        public IList<Msg> Fetch(int batchSize, int maxWaitMillis)
        {
            IList<Msg> messages = new List<Msg>(batchSize);

            PullNoWait(batchSize);
            Read(batchSize, maxWaitMillis, messages);
            if (messages.Count == 0) {
                PullExpiresIn(batchSize, Duration.OfMillis(maxWaitMillis - 10));
                Read(batchSize, maxWaitMillis, messages);
            }

            return messages;
        }

        private const int SubsequentWaits = 500;

        private void Read(int batchSize, int maxWaitMillis, IList<Msg> messages) {
            try
            {
                Msg msg = NextMessage(maxWaitMillis);
                while (msg != null)
                {
                    if (msg.IsJetStream)
                    {
                        messages.Add(msg);
                        if (messages.Count == batchSize)
                        {
                            break;
                        }
                    }

                    msg = NextMessage(SubsequentWaits);
                }
            }
            catch (NATSTimeoutException)
            {
                // it's fine, just end
            }
        }
    }
}
