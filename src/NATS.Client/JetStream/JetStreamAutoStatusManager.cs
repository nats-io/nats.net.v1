using System;
using System.Collections.Generic;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public class JetStreamAutoStatusManager
    {
        private static readonly IList<int> PullKnownStatusCodes = new List<int>(new []{404, 408});
        private const int Threshold = 3;

        private Connection conn;
        private IJetStreamSubscription sub;

        internal bool SyncMode { get; }
        internal bool QueueMode { get; }
        internal bool Pull { get; }
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
        
        internal JetStreamAutoStatusManager(Connection conn, SubscribeOptions so,
            ConsumerConfiguration cc, bool queueMode, bool syncMode)
        {
            this.conn = conn;
            SyncMode = syncMode;
            QueueMode = queueMode;
            LastStreamSeq = 0;
            LastConsumerSeq = 0;
            ExpectedConsumerSeq = 1; // always starts at 1
            LastMsgReceived = -1;

            Pull = so.Pull;

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

            HeartbeatAlarmEventHandler = conn.Opts.HeartbeatAlarmEventHandler ?? DefaultEventHandler.HandleHeartbeatAlarmEvent();
            MsgGapDetectedEventHandler = conn.Opts.MessageGapDetectedEventHandler ?? DefaultEventHandler.HandleMessageGapDetectedEvent();
            UnhandledStatusEventHandler = conn.Opts.UnhandledStatusEventHandler ?? DefaultEventHandler.HandleUnhandledStatusEvent();
        }

        // chicken or egg situation here. The handler needs the sub in case of error
        // but the sub needs the handler in order to be created
        void SetSub(IJetStreamSubscription sub)
        {
            this.sub = sub;
            if (Hb) {
                conn.SetBeforeQueueProcessor(BeforeQueueProcessor);
                asmTimer = new AsmTimer();
            }
        }

        void Shutdown() {
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

        internal bool Manage(Msg msg) {
            if (Pull ? CheckStatusForPullMode(msg) : CheckStatusForPushMode(msg)) {
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
                MsgGapDetectedEventHandler.Invoke(this, new MessageGapDetectedEventArgs(conn, sub,
                    LastStreamSeq, LastConsumerSeq, ExpectedConsumerSeq, receivedConsumerSeq));
            
                if (SyncMode) {
                    throw new NATSJetStreamGapException(sub, ExpectedConsumerSeq, receivedConsumerSeq);
                }
            }
            LastStreamSeq = msg.MetaData.StreamSequence;
            LastConsumerSeq = receivedConsumerSeq;
            ExpectedConsumerSeq = receivedConsumerSeq + 1;
        }

        private Msg BeforeQueueProcessor(Msg msg)
        {
            LastMsgReceived = DateTime.Now.Millisecond;
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
                UnhandledStatusEventHandler.Invoke(this, new UnhandledStatusEventArgs(conn, sub, msg.Status));
                if (SyncMode) 
                {
                    throw new NATSJetStreamStatusException(sub, msg.Status);
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

        private bool CheckStatusForPullMode(Msg msg) {
            if (msg.HasStatus) {
                if ( !PullKnownStatusCodes.Contains(msg.Status.Code) ) {
                    // pull is always sync
                    throw new NATSJetStreamStatusException(sub, msg.Status);
                }
                return true;
            }
            return false;
        }
    }
}