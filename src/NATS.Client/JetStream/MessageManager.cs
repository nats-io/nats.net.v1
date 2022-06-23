// Copyright 2022 The NATS Authors
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
using System.Collections.Generic;
using System.Threading;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    internal abstract class MessageManager
    {
        protected Subscription Sub;

        internal virtual void Startup(Subscription sub)
        {
            Sub = sub;
        }

        internal virtual void Shutdown()
        {
        }

        internal abstract bool Manage(Msg msg);

        internal static void Startup(Subscription sub, MessageManager[] messageManagers)
        {
            foreach (MessageManager mm in messageManagers)
            {
                mm.Startup(sub);
            }
        }

        internal static void Shutdown(MessageManager[] messageManagers)
        {
            foreach (MessageManager mm in messageManagers)
            {
                mm.Shutdown();
            }
        }
    }

    internal class PullMessageManager : MessageManager
    {
        private static readonly IList<int> PullKnownStatusCodes = new List<int>(new []{404, 408, 409});

        internal override bool Manage(Msg msg)
        {
            if (!msg.HasStatus) { return false; }
            
            if ( !PullKnownStatusCodes.Contains(msg.Status.Code) ) {
                // pull is always sync
                throw new NATSJetStreamStatusException(Sub, msg.Status);
            }
            return true;
        }
    }

    internal class PushMessageManager : MessageManager
    {
        private static readonly IList<int> PushKnownStatusCodes = new List<int>(new []{409});
        private const int Threshold = 3;

        private readonly Connection conn;

        internal bool SyncMode { get; }
        internal bool QueueMode { get; }
        internal bool Hb { get; }
        internal bool Fc { get; }

        internal int IdleHeartbeatSetting { get; }
        internal int AlarmPeriodSetting { get; }

        internal string LastFcSubject { get; private set; }
        internal ulong LastStreamSeq { get; private set; }
        internal ulong LastConsumerSeq { get; private set; }
        internal long LastMsgReceived { get; private set; }

        private HeartbeatTimer heartbeatTimer;
        
        internal PushMessageManager(Connection conn, SubscribeOptions so,
            ConsumerConfiguration cc, bool queueMode, bool syncMode)
        {
            this.conn = conn;
            SyncMode = syncMode;
            QueueMode = queueMode;
            LastStreamSeq = 0;
            LastConsumerSeq = 0;
            LastMsgReceived = -1;

            if (queueMode) {
                Hb = false;
                Fc = false;
                IdleHeartbeatSetting = 0;
                AlarmPeriodSetting = 0;
            }
            else
            {
                IdleHeartbeatSetting = cc.IdleHeartbeat?.Millis ?? 0;
                if (IdleHeartbeatSetting <= 0) {
                    AlarmPeriodSetting = 0;
                    Hb = false;
                }
                else {
                    int mat = so.MessageAlarmTime;
                    if (mat < IdleHeartbeatSetting) {
                        AlarmPeriodSetting = IdleHeartbeatSetting * Threshold;
                    }
                    else {
                        AlarmPeriodSetting = mat;
                    }
                    Hb = true;
                }
                Fc = Hb && cc.FlowControl; // can't have fc w/o heartbeat
            }
        }

        // chicken or egg situation here. The handler needs the sub in case of error
        // but the sub needs the handler in order to be created
        internal override void Startup(Subscription sub)
        {
            base.Startup(sub);
            if (Hb) {
                Sub.BeforeChannelAddCheck = BeforeChannelAddCheck;
                heartbeatTimer = new HeartbeatTimer(this);
            }
        }

        internal override void Shutdown() {
            if (heartbeatTimer != null) {
                heartbeatTimer.Shutdown();
                heartbeatTimer = null;
            }
            base.Shutdown();
        }

        class HeartbeatTimer
        {
            private readonly object mu = new object();
            private Timer timer;

            public HeartbeatTimer(PushMessageManager pushMm)
            {
                timer = new Timer(s => {
                        long sinceLast = DateTimeOffset.Now.ToUnixTimeMilliseconds() - pushMm.LastMsgReceived;
                        if (sinceLast > pushMm.AlarmPeriodSetting) {
                            pushMm.conn.Opts.HeartbeatAlarmEventHandlerOrDefault.Invoke(this, 
                                new HeartbeatAlarmEventArgs(pushMm.conn, pushMm.Sub, pushMm.LastStreamSeq, pushMm.LastConsumerSeq));
                        }
                    }, 
                    null, pushMm.AlarmPeriodSetting, pushMm.AlarmPeriodSetting);
            }

            public void Shutdown() {
                if (timer != null)
                {
                    lock (mu)
                    {
                        timer.Dispose();
                        timer = null;
                    }
                }
            }
        }

        internal override bool Manage(Msg msg) {
            // this checks fc, hb and unknown
            // only process fc and hb if those flags are set
            // otherwise they are simply known statuses
            if (msg.HasStatus) {
                if (msg.Status.IsFlowControl()) 
                {
                    if (Fc) {
                        _processFlowControl(msg.Reply, FlowControlSource.FlowControl);
                    }
                }
                else if (msg.Status.IsHeartbeat()) 
                {
                    if (Fc) {
                        _processFlowControl(msg.Header?[JetStreamConstants.ConsumerStalledHeader], FlowControlSource.Heartbeat);
                    }
                }
                else if (!PushKnownStatusCodes.Contains(msg.Status.Code))
                {
                    // If this status is unknown to us, always use the error handler.
                    // this status is unknown to us, always use the error handler.
                    // If it's a sync call, also throw an exception
                    conn.Opts.UnhandledStatusEventHandlerOrDefault.Invoke(this,
                        new UnhandledStatusEventArgs(conn, Sub, msg.Status));
                    if (SyncMode)
                    {
                        throw new NATSJetStreamStatusException(Sub, msg.Status);
                    }
                }
                return true;
            }
            
            // JS Message
            LastStreamSeq = msg.MetaData.StreamSequence;
            LastConsumerSeq = msg.MetaData.ConsumerSequence;;
            return false;
        }

        protected virtual Msg BeforeChannelAddCheck(Msg msg)
        {
            LastMsgReceived = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            if (msg.HasStatus 
                && msg.Status.IsHeartbeat()
                && msg.Header?[JetStreamConstants.ConsumerStalledHeader] == null) 
            {
                    return null; // plain heartbeat, no need to queue
            }
            return msg;
        }

        private void _processFlowControl(string fcSubject, FlowControlSource source) {
            // we may get multiple fc/hb messages with the same reply
            // only need to post to that subject once
            if (fcSubject != null && !fcSubject.Equals(LastFcSubject)) {
                conn.Publish(fcSubject, null);
                LastFcSubject = fcSubject; // set after publish in case the pub fails
                conn.Opts.FlowControlProcessedEventHandlerOrDefault.Invoke(this, new FlowControlProcessedEventArgs(conn, Sub, fcSubject, source));
            }
        }
    }

    internal class SidCheckManager : MessageManager {

        internal override bool Manage(Msg msg)
        {
            return Sub.Sid != msg.Sid;
        }
    }

    internal class OrderedMessageManager : MessageManager
    {
        private readonly JetStream _js;
        private readonly String _stream;
        private readonly ConsumerConfiguration _serverCc;
        private readonly bool _syncMode;

        private ulong lastStreamSeq;
        private ulong expectedConsumerSeq;

        public OrderedMessageManager(JetStream js, string stream, ConsumerConfiguration serverCc, bool syncMode)
        {
            _js = js;
            _stream = stream;
            _serverCc = serverCc;
            _syncMode = syncMode;
            lastStreamSeq = ulong.MaxValue;
            expectedConsumerSeq = 1; // always starts at 1
        }

        internal override bool Manage(Msg msg)
        {
            if (msg == null)
            {
                return false;
            }
            
            ulong receivedConsumerSeq = msg.MetaData.ConsumerSequence;
            if (expectedConsumerSeq != receivedConsumerSeq)
            {
                try
                {
                    expectedConsumerSeq = 1; // consumer always starts with consumer sequence 1

                    // 1. shutdown the managers, for instance stops heartbeat timers
                    MessageManager[] managers = {};
                    if (Sub is JetStreamAbstractSyncSubscription syncSub)
                    {
                        managers = syncSub.messageManagers;
                    }
                    else if (Sub is JetStreamPushAsyncSubscription asyncSub)
                    {
                        managers = asyncSub.messageManagers;
                    }

                    Shutdown(managers);

                    // 2. re-subscribe. This means kill the sub then make a new one
                    //    New sub needs a new deliver subject
                    string newDeliverSubject = Sub.Connection.NewInbox();
                    Sub.reSubscribe(newDeliverSubject);

                    // 3. make a new consumer using the same deliver subject but
                    //    with a new starting point
                    ConsumerConfiguration userCc = ConsumerConfiguration.Builder(_serverCc)
                        .WithDeliverPolicy(DeliverPolicy.ByStartSequence)
                        .WithDeliverSubject(newDeliverSubject)
                        .WithStartSequence(lastStreamSeq + 1)
                        .WithStartTime(DateTime.MinValue) // clear start time in case it was originally set
                        .Build();

                    _js.AddOrUpdateConsumerInternal(_stream, userCc);

                    // 4. re start the managers.
                    Startup(Sub, managers);
                }
                catch (Exception e)
                {
                    NATSBadSubscriptionException bad =
                        new NATSBadSubscriptionException("Ordered subscription fatal error: " + e.Message);
                    ((Connection)_js.Conn).ScheduleErrorEvent(this, bad, Sub);
                    if (_syncMode)
                    {
                        throw bad;
                    }                                                
                }

                return true;
            }
            
            lastStreamSeq = msg.MetaData.StreamSequence;
            expectedConsumerSeq++;
            return false;
        }
    }
}