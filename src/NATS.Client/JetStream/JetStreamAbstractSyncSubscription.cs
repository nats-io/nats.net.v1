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
        internal IAutoStatusManager _asm;
        public JetStream Context { get; }
        public string Stream { get; }
        public string Consumer { get; }
        public string DeliverSubject { get; }

        internal JetStreamAbstractSyncSubscription(Connection conn, string subject, string queue,
            IAutoStatusManager asm, JetStream js, string stream, string consumer, string deliver)
            : base(conn, subject, queue)
        {
            _asm = asm;
            Context = js;
            Stream = stream;
            Consumer = consumer;
            DeliverSubject = deliver;
        }

        public ConsumerInfo GetConsumerInformation() => Context.LookupConsumerInfo(Stream, Consumer);
                
        public override void Unsubscribe()
        {
            _asm.Shutdown();
            base.Unsubscribe();
        }

        internal override void close()
        {
            _asm.Shutdown();
            base.close();
        }

        public new Msg NextMessage()
        {
            // this calls is intended to block indefinitely so if there is a status
            // message it's like not getting a message at all and we keep waiting
            Msg msg = NextMessageImpl(-1);
            while (msg != null && _asm.Manage(msg)) {
                msg = NextMessageImpl(-1);
            }
            return msg;
        }

        protected const int MinMillis = 20;
        protected const int ExpireLessMillis = 10;

        public new Msg NextMessage(int timeout)
        {
            // < 0 means indefinite
            if (timeout < 0)
            {
                return NextMessage();
            }

            // 0 or very short? Try again once if it's a managed message
            if (timeout < MinMillis)
            {
                Msg msg = NextMessageImpl(timeout);
                return _asm.Manage(msg) ? NextMessageImpl(MinMillis) : msg;
            }
            
            // int timeLeft = Math.Max(timeout, MinMillis);
            Stopwatch sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeout) {
                // NMI timeout is the larger of Min or the time left.
                Msg msg = NextMessageImpl( Math.Max(MinMillis, timeout - (int)sw.ElapsedMilliseconds) );
                if (!_asm.Manage(msg)) { // not managed means JS Message
                    return msg;
                }
            }
            
            throw new NATSTimeoutException();
        }
    }
}
