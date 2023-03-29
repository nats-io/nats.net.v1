﻿// Copyright 2022 The NATS Authors
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
    public class OrderedMessageManager : PushMessageManager
    {
        protected ulong ExpectedExternalConsumerSeq;
        protected long? TargetSid;

        public OrderedMessageManager(Connection conn, 
            JetStream js, 
            string stream,
            SubscribeOptions so,
            ConsumerConfiguration originalCc, 
            bool queueMode, 
            bool syncMode) : base(conn, js, stream, so, originalCc, queueMode, syncMode)
        {
            ExpectedExternalConsumerSeq = 1; // always starts at 1
        }

        public override void Startup(IJetStreamSubscription sub)
        {
            base.Startup(sub);
            TargetSid = Sub.Sid;
        }

        public override bool Manage(Msg msg)
        {
            if (msg.Sid != TargetSid)
            {
                return true; // wrong sid is throwaway from previous consumer that errored
            }
            
            if (msg.IsJetStream)
            {
                ulong receivedConsumerSeq = msg.MetaData.ConsumerSequence;
                if (ExpectedExternalConsumerSeq != receivedConsumerSeq) {
                    HandleErrorCondition();
                    return true;
                }
                TrackJsMessage(msg);
                ExpectedExternalConsumerSeq++;
                return false;
            }
            
            ManageStatus(msg);
            return true; // all statuses are managed
        }
        
        private void HandleErrorCondition()
        {
            try
            {
                TargetSid = null;
                ExpectedExternalConsumerSeq = 1; // consumer always starts with consumer sequence 1

                // 1. shutdown the managers, for instance stops heartbeat timers
                Shutdown();

                // 2. re-subscribe. This means kill the sub then make a new one
                //    New sub needs a new deliver subject
                string newDeliverSubject = Sub.Connection.NewInbox();
                ((Subscription)Sub).reSubscribe(newDeliverSubject);
                TargetSid = ((Subscription)Sub).Sid; 

                // 3. make a new consumer using the same deliver subject but
                //    with a new starting point
                ConsumerConfiguration userCc = ConsumerConfiguration.Builder(OriginalCc)
                    .WithDeliverPolicy(DeliverPolicy.ByStartSequence)
                    .WithDeliverSubject(newDeliverSubject)
                    .WithStartSequence(LastStreamSeq + 1)
                    .WithStartTime(DateTime.MinValue) // clear start time in case it was originally set
                    .Build();

                Js.AddOrUpdateConsumerInternal(Stream, userCc);

                // 4. restart the manager.
                Startup(Sub);
            }
            catch (Exception e)
            {
                NATSBadSubscriptionException bad =
                    new NATSBadSubscriptionException("Ordered subscription fatal error: " + e.Message);
                ((Connection)Js.Conn).ScheduleErrorEvent(this, bad, (Subscription)Sub);
                if (SyncMode)
                {
                    throw bad;
                }
            }
        }
    }
}
