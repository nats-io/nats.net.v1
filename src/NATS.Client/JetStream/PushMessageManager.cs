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
    internal class PushMessageManager : MessageManager
    {
        private static readonly IList<int> PushKnownStatusCodes = new List<int>(new []{409});
        private const int Threshold = 3;

        internal readonly Connection _conn;
        internal readonly JetStream _js;
        internal readonly String _stream;
        internal readonly ConsumerConfiguration _serverCc;

        internal bool SyncMode { get; }
        internal bool QueueMode { get; }
        internal bool Hb { get; }
        internal bool Fc { get; }

        internal int IdleHeartbeatSetting { get; }
        internal int AlarmPeriodSetting { get; }

        internal ulong _lastStreamSeq;
        internal ulong _lastConsumerSeq;
        internal long _lastMsgReceived;
        internal string _lastFcSubject;

        private HeartbeatTimer heartbeatTimer;
        
        internal PushMessageManager(Connection conn, 
            JetStream js, 
            string stream,
            SubscribeOptions so,
            ConsumerConfiguration serverCc, 
            bool queueMode, 
            bool syncMode)
        {
            _conn = conn;
            _js = js;
            _stream = stream;
            _serverCc = serverCc;

            SyncMode = syncMode;
            QueueMode = queueMode;
            _lastStreamSeq = 0;
            _lastConsumerSeq = 0;
            _lastMsgReceived = -1;

            if (queueMode) {
                Hb = false;
                Fc = false;
                IdleHeartbeatSetting = 0;
                AlarmPeriodSetting = 0;
            }
            else
            {
                IdleHeartbeatSetting = serverCc.IdleHeartbeat?.Millis ?? 0;
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
                Fc = Hb && serverCc.FlowControl; // can't have fc w/o heartbeat
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

        internal virtual void HandleHeartbeatError()
        {
            _conn.Opts.HeartbeatAlarmEventHandlerOrDefault.Invoke(this, 
                new HeartbeatAlarmEventArgs(_conn, Sub, _lastStreamSeq, _lastConsumerSeq));
            
        }
        
        class HeartbeatTimer
        {
            private readonly object mu = new object();
            private Timer timer;

            public HeartbeatTimer(PushMessageManager pushMm)
            {
                timer = new Timer(s => {
                        long sinceLast = DateTimeOffset.Now.ToUnixTimeMilliseconds() - pushMm._lastMsgReceived;
                        if (sinceLast > pushMm.AlarmPeriodSetting)
                        {
                            pushMm.HandleHeartbeatError();
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

        protected virtual bool SubManage(Msg msg)
        {
            return false;
        }

        internal override bool Manage(Msg msg) {
            if (Sub.Sid != msg.Sid) {
                return true;
            }

            if (msg.HasStatus) {
                // this checks fc, hb and unknown
                // only process fc and hb if those flags are set
                // otherwise they are simply known statuses
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
                    _conn.Opts.UnhandledStatusEventHandlerOrDefault.Invoke(this,
                        new UnhandledStatusEventArgs(_conn, Sub, msg.Status));
                    if (SyncMode)
                    {
                        throw new NATSJetStreamStatusException(Sub, msg.Status);
                    }
                }
                return true;
            }
            
            if (SubManage(msg)) {
                return true;
            }

            // JS Message
            _lastStreamSeq = msg.MetaData.StreamSequence;
            _lastConsumerSeq = msg.MetaData.ConsumerSequence;;
            return false;
        }

        protected virtual Msg BeforeChannelAddCheck(Msg msg)
        {
            _lastMsgReceived = DateTimeOffset.Now.ToUnixTimeMilliseconds();
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
            if (fcSubject != null && !fcSubject.Equals(_lastFcSubject)) {
                _conn.Publish(fcSubject, null);
                _lastFcSubject = fcSubject; // set after publish in case the pub fails
                _conn.Opts.FlowControlProcessedEventHandlerOrDefault.Invoke(this, new FlowControlProcessedEventArgs(_conn, Sub, fcSubject, source));
            }
        }
    }
}
