using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    internal class PullMessageManager : MessageManager
    {
        internal int pendingMessages;
        internal long pendingBytes;
        internal bool trackingBytes;
        protected bool raiseStatusWarnings;
        protected ITrackPendingListener trackPendingListener;

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

        public override void StartPullRequest(string pullId, PullRequestOptions pro, bool raiseStatusWarnings,
            ITrackPendingListener trackPendingListener)
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
                bool zero = pendingMessages < 1;
                if (trackingBytes) {
                    pendingBytes -= b;
                    zero |= pendingBytes < 1;
                }
                if (zero) {
                    pendingMessages = 0;
                    pendingBytes = 0;
                    trackingBytes = false;
                    if (Hb) {
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

            return true;
        }

        public override ManageResult Manage(Msg msg)
        {
            // normal js message
            if (!msg.HasStatus) 
            {
                TrackJsMessage(msg);
                return ManageResult.MESSAGE;
            }

            switch (msg.Status.Code)
            {
                case NatsConstants.NotFoundCode: 
                case NatsConstants.RequestTimeoutCode:
                    if (raiseStatusWarnings)
                    {
                        Conn.Opts.PullStatusWarningEventHandlerOrDefault.Invoke(this, new StatusEventArgs(Conn, (Subscription)Sub, msg.Status));
                    }
                    return ManageResult.TERMINUS;
                
                case NatsConstants.ConflictCode:
                    string statMsg = msg.Status.Message;
                    
                    // sometimes just a warning
                    if (statMsg.StartsWith("Exceeded Max")) {
                        if (raiseStatusWarnings)
                        {
                            Conn.Opts.PullStatusWarningEventHandlerOrDefault.Invoke(this, new StatusEventArgs(Conn, (Subscription)Sub, msg.Status));
                        }
                        return ManageResult.STATUS;
                    }

                    if (statMsg.Equals(JetStreamConstants.BatchCompleted) ||
                        statMsg.Equals(JetStreamConstants.MessageSizeExceedsMaxBytes))
                    {
                        return ManageResult.TERMINUS;
                    }
                    break;
            }
            
            // fall through, all others are errors
            Conn.Opts.PullStatusErrorEventHandlerOrDefault.Invoke(this, new StatusEventArgs(Conn, (Subscription)Sub, msg.Status));
            if (SyncMode)
            {
                throw new NATSJetStreamStatusException((Subscription)Sub, msg.Status);
            }
            return ManageResult.ERROR;
        }
    }
}
