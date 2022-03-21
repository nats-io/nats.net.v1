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
using System.Diagnostics;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public class JetStreamPullSubscription : JetStreamAbstractSyncSubscription, IJetStreamPullSubscription
    {
        internal JetStreamPullSubscription(Connection conn, string subject,
            IAutoStatusManager asm, JetStream js, string stream, string consumer, string deliver)
            : base(conn, subject, null, asm, js, stream, consumer, deliver) {}

        public bool IsPullMode() => true;
        
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

        public void PullNoWait(int batchSize, int expiresInMillis)
        {
            PullInternal(batchSize, true, Duration.OfMillis(expiresInMillis));
        }

        private void PullInternal(int batchSize, bool noWait, Duration expiresIn) {
            int batch = Validator.ValidatePullBatchSize(batchSize);
            string subj = string.Format(JetStreamConstants.JsapiConsumerMsgNext, Stream, Consumer);
            string publishSubject = Context.PrependPrefix(subj);
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
            IList<Msg> messages = new List<Msg>();
            int batchLeft = batchSize;
            
            Stopwatch sw = Stopwatch.StartNew();

            Duration expires = Duration.OfMillis(
                maxWaitMillis > MinMillis
                    ? maxWaitMillis - ExpireLessMillis
                    : maxWaitMillis);
            PullInternal(batchLeft, false, expires);

            try
            {
                // timeout > 0 process as many messages we can in that time period
                // If we get a message that either manager handles, we try again, but
                // with a shorter timeout based on what we already used up
                int timeLeft = maxWaitMillis;
                while (batchLeft > 0 && timeLeft > 0) {
                    Msg msg = NextMessageImpl(timeLeft);
                    if (!_asm.Manage(msg)) { // not managed means JS Message
                        messages.Add(msg);
                        batchLeft--;
                    }
                    // try again while we have time
                    timeLeft = maxWaitMillis - (int)sw.ElapsedMilliseconds;
                }
            }
            catch (NATSTimeoutException)
            {
                // regular timeout, just end
            }

            return messages;
        }
    }
}
