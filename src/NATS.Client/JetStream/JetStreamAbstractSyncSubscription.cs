// Copyright 2021-2023 The NATS Authors
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

using System.Diagnostics;

namespace NATS.Client.JetStream
{
    public abstract class JetStreamAbstractSyncSubscription : SyncSubscription
    {
        internal readonly MessageManager MessageManager;
        internal string _consumer;
        
        // properties of IJetStreamSubscription, needed in subclass implementation
        public JetStream Context { get; }
        public string Stream { get; }
        public string Consumer => _consumer;
        public string DeliverSubject { get; }
        
        internal JetStreamAbstractSyncSubscription(Connection conn, string subject, string queue,
            JetStream js, string stream, string consumer, string deliver, MessageManager messageManager)
            : base(conn, subject, queue)
        {
            Context = js;
            Stream = stream;
            _consumer = consumer; // might be null, JetStream will call UpdateConsumer later
            DeliverSubject = deliver;
            MessageManager = messageManager;
            MessageManager.Startup((IJetStreamSubscription)this);
        }

        internal virtual void UpdateConsumer(string consumer)
        {
            _consumer = consumer;
        }

        public ConsumerInfo GetConsumerInformation() => Context.LookupConsumerInfo(Stream, Consumer);
        
        public override void Unsubscribe()
        {
            MessageManager.Shutdown();
            base.Unsubscribe();
        }

        public override void AutoUnsubscribe(int max)
        {
            MessageManager.Shutdown();
            base.AutoUnsubscribe(max);
        }

        internal override void close()
        {
            MessageManager.Shutdown();
            base.close();
        }

        protected override void Dispose(bool disposing)
        {
            MessageManager.Shutdown();
            base.Dispose(disposing);
        }

        public override Msg NextMessage()
        {
            return _nextUnmanagedWaitForever(null);
        }

        public override Msg NextMessage(int timeout)
        {
            if (timeout < 0)
            {
                return _nextUnmanagedWaitForever(null);
            }
            if (timeout == 0)
            {
                return _nextUnmanagedNoWait(null);
            }
            return _nextUnmanaged(timeout, null);
        }

        internal Msg _nextUnmanagedWaitForever(string expectedPullSubject)
        {
            // this calls is intended to block indefinitely so if there is a managed
            // message it's like not getting a message at all and we keep waiting
            while (true)
            {
                Msg msg = NextMessageImpl(-1);
                switch (MessageManager.Manage(msg))
                {
                    case ManageResult.Message:
                        return msg;
                    case ManageResult.StatusError:
                        // if the status applies throw exception, otherwise it's ignored, fall through
                        if (expectedPullSubject == null || expectedPullSubject.Equals(msg.Subject))
                        {
                            throw new NATSJetStreamStatusException(msg.Status, this);
                        }

                        break;
                }
                // Check again since waiting forever when:
                // 1. Any StatusHandled or StatusTerminus 
                // 2. StatusError that aren't for expected pullSubject
            }
        }

        internal Msg _nextUnmanagedNoWait(string expectedPullSubject)
        {
            while (true) {
                Msg msg = NextMessageImpl(0);
                switch (MessageManager.Manage(msg)) {
                    case ManageResult.Message:
                        return msg;
                    case ManageResult.StatusTerminus:
                        // if the status applies return null, otherwise it's ignored, fall through
                        if (expectedPullSubject == null || expectedPullSubject.Equals(msg.Subject)) 
                        {
                            throw new NATSTimeoutException();
                        }
                        break;
                    case ManageResult.StatusError:
                        // if the status applies throw exception, otherwise it's ignored, fall through
                        if (expectedPullSubject == null || expectedPullSubject.Equals(msg.Subject)) 
                        {
                            throw new NATSJetStreamStatusException(msg.Status, this);
                        }
                        break;
                }
                // Check again when, regular messages might have arrived
                // 1. Any StatusHandled
                // 2. StatusTerminus or StatusError that aren't for expected pullSubject
            }
        }
        
        internal Msg _nextUnmanaged(int timeout, string expectedPullSubject)
        {
            int timeLeft = timeout;
            Stopwatch sw = Stopwatch.StartNew();
            while (timeLeft > 0)
            {
                Msg msg = NextMessageImpl(timeLeft);
                switch (MessageManager.Manage(msg))
                {
                    case ManageResult.Message:
                        return msg;
                    case ManageResult.StatusTerminus:
                        // if the status applies return null, otherwise it's ignored, fall through
                        if (expectedPullSubject == null || expectedPullSubject.Equals(msg.Subject))
                        {
                            throw new NATSTimeoutException();
                        }
                        break;
                    case ManageResult.StatusError:
                        // if the status applies throw exception, otherwise it's ignored, fall through
                        if (expectedPullSubject == null || expectedPullSubject.Equals(msg.Subject))
                        {
                            throw new NATSJetStreamStatusException(msg.Status, this);
                        }
                        break;
                }
                // anything else, try again while we have time
                timeLeft = timeout - (int)sw.ElapsedMilliseconds;
            }
            throw new NATSTimeoutException();
        }
    }
}
