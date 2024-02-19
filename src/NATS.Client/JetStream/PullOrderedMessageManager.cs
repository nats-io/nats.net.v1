// Copyright 2023 The NATS Authors
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

namespace NATS.Client.JetStream
{
    public class PullOrderedMessageManager : PullMessageManager
    {
        protected readonly ConsumerConfiguration OriginalCc;
        protected readonly JetStream Js;
        protected readonly string Stream;
        protected ulong ExpectedExternalConsumerSeq;
        protected long? TargetSid;

        public PullOrderedMessageManager(Connection conn, JetStream js, string stream,
            SubscribeOptions so, ConsumerConfiguration originalCc, bool syncMode) : base(conn, so, syncMode)
        {
            Js = js;
            Stream = stream;
            OriginalCc = originalCc;
            ExpectedExternalConsumerSeq = 1; // always starts at 1
            TargetSid = null;
        }

        public override void Startup(IJetStreamSubscription sub)
        {
            base.Startup(sub);
            TargetSid = sub.Sid;
        }

        public override ManageResult Manage(Msg msg)
        {
            if (msg.Sid != TargetSid)
            {
                return ManageResult.StatusHandled; // wrong sid is throwaway from previous consumer that errored
            }

            if (msg.IsJetStream)
            {
                ulong receivedConsumerSeq = msg.MetaData.ConsumerSequence;
                if (ExpectedExternalConsumerSeq != receivedConsumerSeq)
                {
                    TargetSid = null;
                    ExpectedExternalConsumerSeq = 1;
                    ResetTracking();
                    if (pullManagerObserver != null)
                    {
                        pullManagerObserver.HeartbeatError();
                    }

                    return ManageResult.StatusHandled;
                }

                TrackJsMessage(msg);
                ExpectedExternalConsumerSeq++;
                return ManageResult.Message;
            }

            return ManageStatus(msg);
        }
    }
}
