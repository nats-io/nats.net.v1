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
using System.Collections.Generic;
using System.Threading;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    internal interface IAutoStatusManager
    {
        void SetSub(Subscription sub);
        void Shutdown();
        bool Manage(Msg msg);
    }

    internal class PullAutoStatusManager : IAutoStatusManager
    {
        private static readonly IList<int> PullKnownStatusCodes = new List<int>(new []{404, 408});
        private Subscription _sub;
        
        public void SetSub(Subscription sub)
        {
            _sub = sub;
        }

        public void Shutdown() { /* nothing to do */ }

        public bool Manage(Msg msg)
        {
            if (!msg.HasStatus) { return false; }
            
            if ( !PullKnownStatusCodes.Contains(msg.Status.Code) ) {
                // pull is always sync
                throw new NATSJetStreamStatusException(_sub, msg.Status);
            }
            return true;
        }
    }

    internal class PushAutoStatusManager : IAutoStatusManager
    {
        private const int Threshold = 3;

        private readonly Connection conn;
        private Subscription _sub;

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

        private AsmTimer asmTimer;
        
        internal PushAutoStatusManager(Connection conn, SubscribeOptions so,
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
        public void SetSub(Subscription sub)
        {
            _sub = sub;
            if (Hb) {
                _sub.BeforeChannelAddCheck = BeforeChannelAddCheck;
                asmTimer = new AsmTimer(this);
            }
        }

        public void Shutdown() {
            if (asmTimer != null) {
                asmTimer.Shutdown();
            }
        }

        class AsmTimer
        {
            private readonly object mu = new object();
            private Timer timer;

            public AsmTimer(PushAutoStatusManager asm)
            {
                timer = new Timer(s => {
                        long sinceLast = DateTimeOffset.Now.ToUnixTimeMilliseconds() - asm.LastMsgReceived;
                        if (sinceLast > asm.AlarmPeriodSetting) {
                            asm.conn.Opts.HeartbeatAlarmEventHandlerOrDefault.Invoke(this, 
                                new HeartbeatAlarmEventArgs(asm.conn, asm._sub, asm.LastStreamSeq, asm.LastConsumerSeq));
                        }
                    }, 
                    null, asm.AlarmPeriodSetting, asm.AlarmPeriodSetting);
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

        public bool Manage(Msg msg) {
            // this checks fc, hb and unknown
            // only process fc and hb if those flags are set
            // otherwise they are simply known statuses
            if (msg.HasStatus) {
                if (msg.Status.IsFlowControl()) 
                {
                    if (Fc) {
                        _processFlowControl(msg.Reply, FlowControlSource.FlowControl);
                    }
                    return true;
                }

                if (msg.Status.IsHeartbeat()) 
                {
                    if (Fc) {
                        _processFlowControl(msg.Header?[JetStreamConstants.ConsumerStalledHeader], FlowControlSource.Heartbeat);
                    }
                    return true;
                    
                }

                // this status is unknown to us, always use the error handler.
                // If it's a sync call, also throw an exception
                conn.Opts.UnhandledStatusEventHandlerOrDefault.Invoke(this, new UnhandledStatusEventArgs(conn, _sub, msg.Status));
                if (SyncMode) 
                {
                    throw new NATSJetStreamStatusException(_sub, msg.Status);
                }
                return true;
            }
            
            // JS Message
            LastStreamSeq = msg.MetaData.StreamSequence;
            LastConsumerSeq = msg.MetaData.ConsumerSequence;;
            return false;
        }

        private Msg BeforeChannelAddCheck(Msg msg)
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
                conn.Opts.FlowControlProcessedEventHandlerOrDefault.Invoke(this, new FlowControlProcessedEventArgs(conn, _sub, fcSubject, source));
            }
        }
   }
}