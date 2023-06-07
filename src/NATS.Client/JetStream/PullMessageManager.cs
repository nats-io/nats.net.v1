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

using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    internal class PullMessageManager : MessageManager
    {
        internal int pendingMessages;
        internal long pendingBytes;
        internal bool trackingBytes;
        internal bool raiseStatusWarnings;
        internal ITrackPendingListener trackPendingListener;

        public PullMessageManager(Connection conn, SubscribeOptions so, bool syncMode) : base(conn, so, syncMode)
        {
            trackingBytes = false;
            pendingMessages = 0;
            pendingBytes = 0;
        }

        public override void Startup(IJetStreamSubscription sub)
        {
            base.Startup(sub);
            ((Subscription)Sub).BeforeChannelAddCheck = BeforeChannelAddCheck;
        }

        public override void StartPullRequest(string pullSubject, PullRequestOptions pro, bool raiseStatusWarnings, ITrackPendingListener trackPendingListener)
        {
            lock (StateChangeLock)
            {
                this.raiseStatusWarnings = raiseStatusWarnings;
                this.trackPendingListener = trackPendingListener;
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
                    ShutdownHeartbeatTimer();
                }
            }
        }

        private void TrackPending(int m, long b)
        {
            lock (StateChangeLock)
            {
                pendingMessages -= m;
                pendingBytes -= b;
                if (pendingMessages < 1 || (trackingBytes && pendingBytes < 1))
                {
                    pendingMessages = 0;
                    pendingBytes = 0L;
                    trackingBytes = false;
                    if (Hb)
                    {
                        ShutdownHeartbeatTimer();
                    }
                }
                if (trackPendingListener != null) {
                    trackPendingListener.Track(pendingMessages, pendingBytes, trackingBytes);
                }
            }
        }

        protected override bool BeforeChannelAddCheck(Msg msg)
        {
            MessageReceived(); // record message time. Used for heartbeat tracking

            MsgStatus status = msg.Status;

            // normal js message
            if (!msg.HasStatus) 
            {
                TrackPending(1, msg.ConsumeByteCount);
                return true;
            }
            // heartbeat just needed to be recorded
            if (status.IsHeartbeat()) {
                return false;
            }

            string s = msg.Header[JetStreamConstants.NatsPendingMessages];
            int m;
            if (s != null && int.TryParse(s, out m))
            {
                long b;
                s = msg.Header[JetStreamConstants.NatsPendingBytes];
                if (s != null && long.TryParse(s, out b))
                {
                    TrackPending(m, b);
                }
            }

            // not found or timeout only have message/byte tracking, so no need for them to be queued (return false)
            // all other statuses are either warnings or errors and handled in manage
            return status.Code != NatsConstants.NotFoundCode && status.Code != NatsConstants.RequestTimeoutCode;
        }

        public override ManageResult Manage(Msg msg)
        {
            // normal js message
            if (!msg.HasStatus) 
            {
                TrackJsMessage(msg);
                return ManageResult.Message;
            }

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
            Conn.Opts.PullStatusErrorEventHandlerOrDefault.Invoke(this, new StatusEventArgs(Conn, (Subscription)Sub, msg.Status));
            if (SyncMode)
            {
                throw new NATSJetStreamStatusException(msg.Status, (Subscription)Sub);
            }

            return ManageResult.StatusError;
        }
    }
}
