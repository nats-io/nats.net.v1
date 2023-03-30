using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    internal class PullMessageManager : MessageManager
    {
        protected long pendingMessages;
        protected long pendingBytes;
        protected bool trackingBytes;

        public PullMessageManager(Connection conn, bool syncMode) : base(conn, syncMode)
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

        public override void StartPullRequest(PullRequestOptions pro)
        {
            lock (StateChangeLock)
            {
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

        private void TrackPending(long m, long b)
        {
            lock (StateChangeLock)
            {
                pendingMessages -= m;
                pendingBytes -= b;
                if (pendingMessages < 1 || (trackingBytes && pendingBytes < 1))
                {
                    pendingMessages = 0L;
                    pendingBytes = 0L;
                    trackingBytes = false;
                    if (Hb)
                    {
                        ShutdownHeartbeatTimer();
                    }
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
                TrackPending(1, BytesInMessage(msg));
                return true;
            }

            // heartbeat just needed to be recorded
            if (status.IsHeartbeat()) {
                return false;
            }

            string s = msg.Header[JetStreamConstants.NatsPendingMessages];
            long m;
            if (s != null && long.TryParse(s, out m))
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

        public override bool Manage(Msg msg)
        {
            // normal js message
            if (!msg.HasStatus) 
            {
                TrackJsMessage(msg);
                return false;
            }

            if (msg.Status.Code == NatsConstants.ConflictCode) {
                // sometimes just a warning
                if (msg.Status.Message.Contains("Exceed")) {
                    Conn.Opts.PullStatusWarningEventHandlerOrDefault.Invoke(this, new StatusEventArgs(Conn, (Subscription)Sub, msg.Status));
                    return true;
                }
                // fall through
            }

            // all others are errors
            Conn.Opts.PullStatusErrorEventHandlerOrDefault.Invoke(this, new StatusEventArgs(Conn, (Subscription)Sub, msg.Status));
            if (SyncMode)
            {
                throw new NATSJetStreamStatusException((Subscription)Sub, msg.Status);
            }

            return true; // all status are managed
        }

        private long BytesInMessage(Msg msg) {
            return msg.Subject.Length
                   + msg.headerLen
                   + msg.Data.Length
                   + (msg.Reply == null ? 0 : msg.Reply.Length);
        }
    }
}
