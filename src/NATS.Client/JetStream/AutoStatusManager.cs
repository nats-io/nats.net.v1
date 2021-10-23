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
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    internal interface IAutoStatusManager
    {
        void SetSub(IJetStreamSubscription sub);
        void Shutdown();
        bool Manage(Msg msg);
    }

    internal class PullAutoStatusManager : IAutoStatusManager
    {
        private static readonly IList<int> PullKnownStatusCodes = new List<int>(new []{404, 408});
        private IJetStreamSubscription _sub;
        
        public void SetSub(IJetStreamSubscription sub)
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
        private IJetStreamSubscription _sub;

        internal bool SyncMode { get; }
        internal bool QueueMode { get; }
        internal bool Gap { get; }
        internal bool Hb { get; }
        internal bool Fc { get; }

        internal long IdleHeartbeatSetting { get; }
        internal long AlarmPeriodSetting { get; }

        internal string LastFcSubject { get; private set; }
        internal ulong LastStreamSeq { get; private set; }
        internal ulong LastConsumerSeq { get; private set; }
        internal ulong ExpectedConsumerSeq { get; private set; }
        internal long LastMsgReceived { get; private set; }
        
        private EventHandler<HeartbeatAlarmEventArgs> HeartbeatAlarmEventHandler;
        private EventHandler<MessageGapDetectedEventArgs> MsgGapDetectedEventHandler;
        private EventHandler<UnhandledStatusEventArgs> UnhandledStatusEventHandler;

        private AsmTimer asmTimer;
        
        internal PushAutoStatusManager(Connection conn, SubscribeOptions so,
            ConsumerConfiguration cc, bool queueMode, bool syncMode)
        {
            this.conn = conn;
            SyncMode = syncMode;
            QueueMode = queueMode;
            LastStreamSeq = 0;
            LastConsumerSeq = 0;
            ExpectedConsumerSeq = 1; // always starts at 1
            LastMsgReceived = -1;

            if (queueMode) {
                Gap = false;
                Hb = false;
                Fc = false;
                IdleHeartbeatSetting = 0;
                AlarmPeriodSetting = 0;
            }
            else
            {
                Gap = so.DetectGaps;
                IdleHeartbeatSetting = cc.IdleHeartbeat.Millis;
                if (IdleHeartbeatSetting == 0) {
                    AlarmPeriodSetting = 0;
                    Hb = false;
                }
                else {
                    long mat = so.MessageAlarmTime;
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

            HeartbeatAlarmEventHandler = conn.Opts.HeartbeatAlarmEventHandler ?? NoOpEventHandler.HandleHeartbeatAlarmEvent();
            MsgGapDetectedEventHandler = conn.Opts.MessageGapDetectedEventHandler ?? NoOpEventHandler.HandleMessageGapDetectedEvent();
            UnhandledStatusEventHandler = conn.Opts.UnhandledStatusEventHandler ?? NoOpEventHandler.HandleUnhandledStatusEvent();
        }

        // chicken or egg situation here. The handler needs the sub in case of error
        // but the sub needs the handler in order to be created
        public void SetSub(IJetStreamSubscription sub)
        {
            _sub = sub;
            if (Hb) {
                conn.SetBeforeQueueProcessor(BeforeQueueProcessor);
                asmTimer = new AsmTimer();
            }
        }

        public void Shutdown() {
            if (asmTimer != null) {
                asmTimer.Shutdown();
            }
        }

        // TODO
        class AsmTimer {

            /* synchronized */ void Restart() {
                cancel();
                // if (sub.isActive()) {
                    // timer = new Timer();
                    // timer.schedule(new TimerWrapperTimerTask(), alarmPeriodSetting);
                // }
            }

            /* synchronized */ public void Shutdown() {
                cancel();
            }

            private void cancel() {
                // if (timer != null) {
                    // timer.cancel();
                    // timer.purge();
                    // timer = null;
                // }
            }
        }

        public bool Manage(Msg msg) {
            if (CheckStatusForPushMode(msg)) {
                return true;
            }
            if (Gap) {
                DetectGaps(msg);
            }
            return false;
        }

        private void DetectGaps(Msg msg)
        {
            ulong receivedConsumerSeq = msg.MetaData.ConsumerSequence;
            if (ExpectedConsumerSeq != receivedConsumerSeq) 
            {
                MsgGapDetectedEventHandler.Invoke(this, new MessageGapDetectedEventArgs(conn, _sub,
                    LastStreamSeq, LastConsumerSeq, ExpectedConsumerSeq, receivedConsumerSeq));
            
                if (SyncMode) {
                    throw new NATSJetStreamGapException(_sub, ExpectedConsumerSeq, receivedConsumerSeq);
                }
            }
            LastStreamSeq = msg.MetaData.StreamSequence;
            LastConsumerSeq = receivedConsumerSeq;
            ExpectedConsumerSeq = receivedConsumerSeq + 1;
        }

        private Msg BeforeQueueProcessor(Msg msg)
        {
            LastMsgReceived = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            if (msg.HasStatus 
                && msg.Status.IsHeartbeat()
                && msg.Header?[JetStreamConstants.ConsumerStalledHdr] == null) 
            {
                    return null; // plain heartbeat, no need to queue
            }
            return msg;
        }

        private bool CheckStatusForPushMode(Msg msg) {
            // this checks fc, hb and unknown
            // only process fc and hb if those flags are set
            // otherwise they are simply known statuses
            if (msg.HasStatus) {
                if (msg.Status.IsFlowControl()) 
                {
                    if (Fc) {
                        _processFlowControl(msg.Reply);
                    }
                    return true;
                }

                if (msg.Status.IsHeartbeat()) 
                {
                    if (Fc) {
                        _processFlowControl(msg.Header?[JetStreamConstants.ConsumerStalledHdr]);
                    }
                    return true;
                    
                }

                // this status is unknown to us, always use the error handler.
                // If it's a sync call, also throw an exception
                UnhandledStatusEventHandler.Invoke(this, new UnhandledStatusEventArgs(conn, _sub, msg.Status));
                if (SyncMode) 
                {
                    throw new NATSJetStreamStatusException(_sub, msg.Status);
                }
                return true;
            }
            return false;
        }

        private void _processFlowControl(String fcSubject) {
            // we may get multiple fc/hb messages with the same reply
            // only need to post to that subject once
            if (fcSubject != null && !fcSubject.Equals(LastFcSubject)) {
                conn.Publish(fcSubject, null);
                LastFcSubject = fcSubject; // set after publish in case the pub fails
            }
        }
   }
}