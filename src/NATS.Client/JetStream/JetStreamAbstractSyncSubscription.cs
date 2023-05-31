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
using System.Diagnostics;

namespace NATS.Client.JetStream
{
    public class JetStreamAbstractSyncSubscription : SyncSubscription
    {
        internal readonly MessageManager MessageManager;
        
        // properties of IJetStreamSubscription
        public JetStream Context { get; }
        public string Stream { get; }
        public string Consumer { get; internal set; }
        public string DeliverSubject { get; }

        internal JetStreamAbstractSyncSubscription(Connection conn, string subject, string queue,
            JetStream js, string stream, string consumer, string deliver, MessageManager messageManager)
            : base(conn, subject, queue)
        {
            Context = js;
            Stream = stream;
            Consumer = consumer; // might be null, someone will call set on ConsumerName
            DeliverSubject = deliver;
            MessageManager = messageManager;
            MessageManager.Startup((IJetStreamSubscription)this);
        }

        public ConsumerInfo GetConsumerInformation() => Context.LookupConsumerInfo(Stream, Consumer);
        
        public override void Unsubscribe()
        {
            MessageManager.Shutdown();
            base.Unsubscribe();
        }

        internal override void close()
        {
            MessageManager.Shutdown();
            base.close();
        }

        public new Msg NextMessage()
        {
            // this calls is intended to block indefinitely so if there is a managed
            // message it's like not getting a message at all and we keep waiting
            Msg msg = NextMessageImpl(-1);
            while (msg != null && MessageManager.Manage(msg)) {
                msg = NextMessageImpl(-1);
            }
            return msg;
        }

        public new Msg NextMessage(int timeout)
        {
            // < 0 means indefinite
            if (timeout < 0)
            {
                return NextMessage();
            }
            
            Stopwatch sw = Stopwatch.StartNew();
            int timeLeft = Math.Max(timeout, 1); // 0 would never enter the loop
            while (timeLeft > 0)
            {
                Msg msg = NextMessageImpl(timeLeft);
                if (!MessageManager.Manage(msg)) { // not managed means JS Message
                    return msg;
                }
                timeLeft = timeout - (int)sw.ElapsedMilliseconds;
            }
            
            throw new NATSTimeoutException();
        }
    }
}
