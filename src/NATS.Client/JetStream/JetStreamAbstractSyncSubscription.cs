﻿// Copyright 2021-2023 The NATS Authors
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

        internal override void close()
        {
            MessageManager.Shutdown();
            base.close();
        }

        public new Msg NextMessage()
        {
            return _nextUnmanagedWaitForever(null);
        }

        public new Msg NextMessage(int timeout)
        {
            if (timeout < 0)
            {
                return _nextUnmanagedWaitForever(null);
            }
            if (timeout == 0)
            {
                return _nextUnmanagedNoWait(null, true);
            }
            return _nextUnmanaged(timeout, null, true);
        }

        protected Msg _nextUnmanagedWaitForever(String expectedPullSubject)
        {
            // this calls is intended to block indefinitely so if there is a managed
            // message it's like not getting a message at all and we keep waiting
            while (true)
            {
                Msg msg = NextMessageImpl(-1);
                if (msg != null)
                {
                    // null shouldn't happen, so just a code guard b/c NextMessageImpl can return null 
                    {
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
                        // StatusHandled, StatusTerminus and StatusError that aren't for expected pullSubject: check again since waiting forever
                    }
                }
            }
        }

        protected Msg _nextUnmanagedNoWait(string expectedPullSubject, bool throwTimeoutOnNull)
        {
            while (true) {
                Msg msg = NextMessageImpl(0);
                if (msg == null) {
                    if (throwTimeoutOnNull)
                    {
                        throw new NATSTimeoutException();
                    }
                    return null;
                }
                switch (MessageManager.Manage(msg)) {
                    case ManageResult.Message:
                        return msg;
                    case ManageResult.StatusTerminus:
                        // if the status applies return null, otherwise it's ignored, fall through
                        if (expectedPullSubject == null || expectedPullSubject.Equals(msg.Subject)) {
                            if (throwTimeoutOnNull)
                            {
                                throw new NATSTimeoutException();
                            }
                            return null;
                        }
                        break;
                    case ManageResult.StatusError:
                        // if the status applies throw exception, otherwise it's ignored, fall through
                        if (expectedPullSubject == null || expectedPullSubject.Equals(msg.Subject)) {
                            throw new NATSJetStreamStatusException(msg.Status, this);
                        }
                        break;
                }
                // StatusHandled: regular messages might have arrived, check again
            }
        }
        
        protected Msg _nextUnmanaged(int timeout, string expectedPullSubject, bool throwTimeoutOnNull)
        {
            int timeLeft = timeout;
            Stopwatch sw = Stopwatch.StartNew();
            while (timeLeft > 0)
            {
                Msg msg = NextMessageImpl(timeLeft);
                if (msg != null)
                {
                    switch (MessageManager.Manage(msg))
                    {
                        case ManageResult.Message:
                            return msg;
                        case ManageResult.StatusTerminus:
                            // if the status applies return null, otherwise it's ignored, fall through
                            if (expectedPullSubject == null || expectedPullSubject.Equals(msg.Subject))
                            {
                                if (throwTimeoutOnNull)
                                {
                                    throw new NATSTimeoutException();
                                }
                                return null;
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
                }
                timeLeft = timeout - (int)sw.ElapsedMilliseconds;
            }
            if (throwTimeoutOnNull)
            {
                throw new NATSTimeoutException();
            }
            return null;
        }

    }
}
