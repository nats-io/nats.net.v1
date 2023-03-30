// Copyright 2022-2023 The NATS Authors
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
using System.Threading;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public abstract class MessageManager
    {
        public const int Threshold = 3;

        protected readonly object StateChangeLock;
        protected readonly Connection Conn;
        protected readonly bool SyncMode;

        protected IJetStreamSubscription Sub; // not readonly it is not set until after construction

        protected ulong LastStreamSeq;
        protected ulong LastConsumerSeq;
        protected long LastMsgReceived;

        protected bool Hb;
        protected int IdleHeartbeatSetting;
        protected int AlarmPeriodSetting;
        protected Timer heartbeatTimer;

        protected MessageManager(Connection conn, bool syncMode)
        {
            StateChangeLock = new object();
            Conn = conn;
            SyncMode = syncMode;
            LastStreamSeq = 0;
            LastConsumerSeq = 0;

            Hb = false;
            IdleHeartbeatSetting = 0;
            AlarmPeriodSetting = 0;

            LastMsgReceived = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        }

        public virtual void Startup(IJetStreamSubscription sub)
        {
            Sub = sub;
        }

        public virtual void Shutdown()
        {
            ShutdownHeartbeatTimer();
        }

        public virtual void StartPullRequest(PullRequestOptions pullRequestOptions) {
            // does nothing - only implemented for pulls, but in base class since instance is always referenced as MessageManager, not subclass
        }

        protected void MessageReceived()
        {
            lock (StateChangeLock)
            {
                LastMsgReceived = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            }
        }

        protected virtual bool BeforeChannelAddCheck(Msg msg)
        {
            return true;
        }

        public abstract bool Manage(Msg msg);
        
        protected void TrackJsMessage(Msg msg) {
            lock (StateChangeLock)
            {
                LastStreamSeq = msg.MetaData.StreamSequence;
                LastConsumerSeq++;
            }
        }
        
        internal virtual void HandleHeartbeatError()
        {
            Conn.Opts.HeartbeatAlarmEventHandlerOrDefault.Invoke(this, 
                new HeartbeatAlarmEventArgs(Conn, (Subscription)Sub, LastStreamSeq, LastConsumerSeq));
            
        }

        protected void ConfigureIdleHeartbeat(Duration configIdleHeartbeat, int configMessageAlarmTime)
        {
            lock (StateChangeLock)
            {
                IdleHeartbeatSetting = configIdleHeartbeat == null ? 0 : configIdleHeartbeat.Millis;
                if (IdleHeartbeatSetting <= 0)
                {
                    AlarmPeriodSetting = 0;
                    Hb = false;
                }
                else
                {
                    if (configMessageAlarmTime < IdleHeartbeatSetting)
                    {
                        AlarmPeriodSetting = IdleHeartbeatSetting * Threshold;
                    }
                    else
                    {
                        AlarmPeriodSetting = configMessageAlarmTime;
                    }

                    Hb = true;
                }
            }
        }

        protected void InitOrResetHeartbeatTimer()
        {
            lock (StateChangeLock)
            {
                ShutdownHeartbeatTimer();
                heartbeatTimer = new Timer(state =>
                    {
                        long sinceLast = DateTimeOffset.Now.ToUnixTimeMilliseconds() - LastMsgReceived;
                        if (sinceLast > AlarmPeriodSetting)
                        {
                            HandleHeartbeatError();
                        }
                    },
                    null, AlarmPeriodSetting, AlarmPeriodSetting);
            }
        }

        protected void ShutdownHeartbeatTimer()
        {
            lock (StateChangeLock)
            {
                if (heartbeatTimer != null)
                {
                    heartbeatTimer.Dispose();
                    heartbeatTimer = null;
                }
            }
        }
    }
}
