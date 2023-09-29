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

using System;

namespace NATS.Client.JetStream
{
    public class PullOrderedMessageManager : PullMessageManager
    {
        protected readonly ConsumerConfiguration OriginalCc;
        protected readonly JetStream Js;
        protected readonly string Stream;
        protected ulong ExpectedExternalConsumerSeq;
        protected long TargetSid;

        public PullOrderedMessageManager(Connection conn, JetStream js, string stream,
            SubscribeOptions so, ConsumerConfiguration originalCc, bool syncMode) : base(conn, so, syncMode) 
        {
            Js = js;
            Stream = stream;
            OriginalCc = originalCc;
            ExpectedExternalConsumerSeq = 1; // always starts at 1
            TargetSid = -1;
        }

        public override void Startup(IJetStreamSubscription sub)
        {
            base.Startup(sub);
            TargetSid = sub.Sid;
        }

        public override ManageResult Manage(Msg msg)
        {
            if (msg.Sid != TargetSid) {
                return ManageResult.StatusHandled; // wrong sid is throwaway from previous consumer that errored
            }
    
            if (msg.IsJetStream) {
                ulong receivedConsumerSeq = msg.MetaData.ConsumerSequence;
                if (ExpectedExternalConsumerSeq != receivedConsumerSeq) {
                    HandleErrorCondition();
                    return ManageResult.StatusHandled;
                }
                TrackJsMessage(msg);
                ExpectedExternalConsumerSeq++;
                return ManageResult.Message;
            }
    
            return ManageStatus(msg);
        }
    
        private void HandleErrorCondition() {
            try
            {
                TargetSid = -1;
                ExpectedExternalConsumerSeq = 1; // consumer always starts with consumer sequence 1
    
                // 1. shutdown the manager, for instance stops heartbeat timers
                Shutdown();
    
                // 2. re-subscribe. This means kill the sub then make a new one
                //    New sub needs a new deliverSubject
                string newDeliverSubject = Sub.Connection.NewInbox();
                ((Subscription)Sub).ReSubscribe(newDeliverSubject);
                TargetSid = Sub.Sid;
    
                // 3. make a new consumer using the same deliver subject but
                //    with a new starting point
                ConsumerConfiguration userCC = ConsumerConfiguration.Builder(OriginalCc)
                    .WithName(Js.GenerateConsumerName())
                    .WithDeliverPolicy(DeliverPolicy.ByStartSequence)
                    .WithDeliverSubject(newDeliverSubject)
                    .WithStartSequence(LastStreamSeq + 1)
                    .WithStartTime(DateTime.MinValue) // clear start time in case it was originally set
                    .Build();
                Js.CreateConsumerInternal(Stream, userCC);
    
                // 4. restart the manager.
                Startup(Sub);
            }
            catch (Exception e) {
                NATSException n = new NATSException("Ordered subscription fatal error: " + e.Message); 
                ((Connection)Js.Conn).ScheduleErrorEvent(this, n, (Subscription)Sub);
                if (SyncMode)
                {
                    throw n;
                }
            }
        }
    }
}
