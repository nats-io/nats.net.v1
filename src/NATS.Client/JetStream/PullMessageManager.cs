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
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public class PullMessageManager : MessageManager
    {
        internal int pendingMessages;
        internal long pendingBytes;
        internal bool trackingBytes;
        internal bool raiseStatusWarnings;
        internal IPullManagerObserver pullManagerObserver;

        public PullMessageManager(Connection conn, SubscribeOptions so, bool syncMode) : base(conn, so, syncMode)
        {
            ResetTracking();
        }

        public override void Startup(IJetStreamSubscription sub)
        {
            base.Startup(sub);
            ((Subscription)Sub).BeforeChannelAddCheck = BeforeChannelAddCheck;
        }

        public override void StartPullRequest(string pullSubject, PullRequestOptions pro, bool raiseStatusWarnings, IPullManagerObserver pullManagerObserver)
        {
            lock (StateChangeLock)
            {
                this.raiseStatusWarnings = raiseStatusWarnings;
                this.pullManagerObserver = pullManagerObserver;
                pendingMessages += pro.BatchSize;
                pendingBytes += pro.MaxBytes;
                trackingBytes = (pendingBytes > 0);
                ConfigureIdleHeartbeat(pro.IdleHeartbeat, -1);
                if (Hb)
                {
                    InitOrResetHeartbeatTimer();
                }
                else
                {
                    ShutdownHeartbeatTimer(); // just in case the pull was changed from hb to non-hb
                }
            }
        }
        
        internal override void HandleHeartbeatError()
        {
            base.HandleHeartbeatError();
            ResetTracking();
            if (pullManagerObserver != null)
            {
                pullManagerObserver.HeartbeatError();
            }
        }

        private void TrackIncoming(int m, long b)
        {
            lock (StateChangeLock)
            {
                // message time used for heartbeat tracking
                UpdateLastMessageReceived();

                if (m != Int32.MinValue)
                {
                    pendingMessages -= m;
                    bool zero = pendingMessages < 1;
                    if (trackingBytes)
                    {
                        pendingBytes -= b;
                        zero |= pendingBytes < 1;
                    }

                    if (zero)
                    {
                        ResetTracking();
                    }

                    if (pullManagerObserver != null)
                    {
                        pullManagerObserver.PendingUpdated();
                    }
                }
            }
        }
        
        internal void ResetTracking() {
            pendingMessages = 0;
            pendingBytes = 0;
            trackingBytes = false;
            UpdateLastMessageReceived();
        }


        protected override bool BeforeChannelAddCheck(Msg msg)
        {
            UpdateLastMessageReceived(); // record message time. Used for heartbeat tracking

            MsgStatus status = msg.Status;

            // normal js message
            if (!msg.HasStatus) 
            {
                TrackIncoming(1, msg.ConsumeByteCount);
                return true;
            }
            
            // heartbeat just needed to be recorded
            if (status.IsHeartbeat()) {
                return false;
            }

            string s = msg.Header[JetStreamConstants.NatsPendingMessages];
            int m = int.MinValue;
            long b = long.MaxValue;
            if (s != null && int.TryParse(s, out m))
            {
                s = msg.Header[JetStreamConstants.NatsPendingBytes];
                if (s == null || !long.TryParse(s, out b))
                {
                    // at this point s should not be null and should parse fine
                    // but if it doesn't, don't fail; make sure don't track m/b
                    m = int.MinValue;
                }
            }
            TrackIncoming(m, b);
            return true;
        }

        public override ManageResult Manage(Msg msg)
        {
            // normal js message
            if (!msg.HasStatus) 
            {
                TrackJsMessage(msg);
                return ManageResult.Message;
            }
            return ManageStatus(msg);
        }

        protected ManageResult ManageStatus(Msg msg)
        {
            switch (msg.Status.Code)
            {
                case NatsConstants.NotFoundCode:
                case NatsConstants.RequestTimeoutCode:
                    if (raiseStatusWarnings)
                    {
                        Conn.Opts.PullStatusWarningEventHandlerOrDefault.Invoke(this,
                            new StatusEventArgs(Conn, (Subscription)Sub, msg.Status));
                    }
                    return ManageResult.StatusTerminus;

                case NatsConstants.ConflictCode:
                    // sometimes just a warning
                    string statMsg = msg.Status.Message;
                    if (statMsg.StartsWith("Exceeded Max"))
                    {
                        if (raiseStatusWarnings)
                        {
                            Conn.Opts.PullStatusWarningEventHandlerOrDefault.Invoke(this,
                                new StatusEventArgs(Conn, (Subscription)Sub, msg.Status));
                        }
                        return ManageResult.StatusHandled;
                    }

                    if (statMsg.Equals(JetStreamConstants.BatchCompleted) ||
                        statMsg.Equals(JetStreamConstants.MessageSizeExceedsMaxBytes))
                    {
                        return ManageResult.StatusTerminus;
                    }
                    break;
            }

            // all others are errors
            Conn.Opts.PullStatusErrorEventHandlerOrDefault.Invoke(this,
                new StatusEventArgs(Conn, (Subscription)Sub, msg.Status));
            return ManageResult.StatusError;
        }

        internal bool NoMorePending() {
            return pendingMessages < 1 || (trackingBytes && pendingBytes < 1);
        }
    }
}
