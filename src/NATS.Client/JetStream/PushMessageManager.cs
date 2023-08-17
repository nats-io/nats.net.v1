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
    public class PushMessageManager : MessageManager
    {
        protected readonly JetStream Js;
        protected readonly string Stream;
        protected readonly ConsumerConfiguration OriginalCc;

        protected readonly bool QueueMode;
        protected readonly bool Fc;
        protected string LastFcSubject;
        
        public PushMessageManager(Connection conn, 
            JetStream js, 
            string stream,
            SubscribeOptions so,
            ConsumerConfiguration originalCc, 
            bool queueMode, 
            bool syncMode) : base(conn, so, syncMode)
        {
            this.Js = js;
            Stream = stream;
            OriginalCc = originalCc;
            QueueMode = queueMode;

            if (queueMode) {
                Fc = false;
            }
            else
            {
                ConfigureIdleHeartbeat(OriginalCc.IdleHeartbeat, so.MessageAlarmTime);
                Fc = Hb && originalCc.FlowControl; // can't have fc w/o heartbeat
            }
        }

        public override void Startup(IJetStreamSubscription sub)
        {
            base.Startup(sub);
            ((Subscription)Sub).BeforeChannelAddCheck = BeforeChannelAddCheck;
            if (Hb) {
                InitOrResetHeartbeatTimer();
            }
        }

        protected override bool BeforeChannelAddCheck(Msg msg)
        {
            if (Hb)
            {
                MessageReceived(); // only need to track when heartbeats are expected
                if (msg.HasStatus)
                {
                    // only fc heartbeats get queued
                    if (msg.Status.IsHeartbeat())
                    {
                        return hasFcSubject(msg); // true if a fc hb
                    }
                }
            }
            return true;
        }

        protected bool hasFcSubject(Msg msg) {
            return msg.Header != null && msg.Header[JetStreamConstants.ConsumerStalledHeader] != null;
        }

        protected string extractFcSubject(Msg msg) {
            return msg.Header == null ? null : msg.Header[JetStreamConstants.ConsumerStalledHeader];
        }

        public override ManageResult Manage(Msg msg) {
            if (!msg.HasStatus)
            {
                TrackJsMessage(msg);
                return ManageResult.Message;
            }
            return ManageStatus(msg);
        }

        protected ManageResult ManageStatus(Msg msg) {
            // this checks fc, hb and unknown
            // only process fc and hb if those flags are set
            // otherwise they are simply known statuses
            if (Fc) {
                bool isFlowControl = msg.Status.IsFlowControl();
                string fcSubject = isFlowControl ? msg.Reply : extractFcSubject(msg);
                if (fcSubject != null) {
                    _processFlowControl(fcSubject, isFlowControl ? FlowControlSource.FlowControl : FlowControlSource.Heartbeat);
                    return ManageResult.StatusHandled;
                }
            }
            Conn.Opts.UnhandledStatusEventHandlerOrDefault.Invoke(this, new UnhandledStatusEventArgs(Conn, (Subscription)Sub, msg.Status));
            return ManageResult.StatusError;
        }

        private void _processFlowControl(string fcSubject, FlowControlSource source) {
            // we may get multiple fc/hb messages with the same reply
            // only need to post to that subject once
            if (fcSubject != null && !fcSubject.Equals(LastFcSubject)) {
                Conn.Publish(fcSubject, null);
                LastFcSubject = fcSubject; // set after publish in case the pub fails
                Conn.Opts.FlowControlProcessedEventHandlerOrDefault.Invoke(this, new FlowControlProcessedEventArgs(Conn, (Subscription)Sub, fcSubject, source));
            }
        }
    }
}
