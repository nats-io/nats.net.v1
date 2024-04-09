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

namespace NATS.Client.JetStream
{
    public class OrderedMessageManager : PushMessageManager
    {
        protected ulong ExpectedExternalConsumerSeq;
        protected long? TargetSid;

        public OrderedMessageManager(
            Connection conn, 
            JetStream js, 
            string stream,
            SubscribeOptions so,
            ConsumerConfiguration originalCc, 
            bool queueMode, 
            bool syncMode) : base(conn, js, stream, so, originalCc, queueMode, syncMode)
        {
            ExpectedExternalConsumerSeq = 1; // always starts at 1
            TargetSid = null;
        }

        public override void Startup(IJetStreamSubscription sub)
        {
            base.Startup(sub);
            TargetSid = Sub.Sid;
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
                if (ExpectedExternalConsumerSeq != receivedConsumerSeq) {
                    HandleErrorCondition();
                    return ManageResult.StatusHandled; // not technically a status
                }
                TrackJsMessage(msg);
                ExpectedExternalConsumerSeq++;
                return ManageResult.Message;
            }
            
            return ManageStatus(msg);
        }
                
        internal override void HandleHeartbeatError()
        {
            base.HandleHeartbeatError();
            HandleErrorCondition();
        }

        private void HandleErrorCondition()
        {
            try
            {
                TargetSid = null;
                ExpectedExternalConsumerSeq = 1; // consumer always starts with consumer sequence 1

                // 1. delete the consumer by name so we can recreate it with a different delivery policy
                //    b/c we cannot edit a push consumer's delivery policy
                IJetStreamManagement jsm = Conn.CreateJetStreamManagementContext(Js.JetStreamOptions);
                String actualConsumerName = Sub.Consumer;
                try {
                    jsm.DeleteConsumer(Stream, actualConsumerName);
                }
                catch (Exception) { /* ignored */ }

                // 2. re-subscribe. This means kill the sub then make a new one
                //    New sub needs a new deliver subject
                string newDeliverSubject = Sub.Connection.NewInbox();
                ((Subscription)Sub).ReSubscribe(newDeliverSubject);
                TargetSid = ((Subscription)Sub).Sid; 

                // 3. make a new consumer using the same deliver subject but
                //    with a new starting point
                ConsumerConfiguration userCc = Js.ConsumerConfigurationForOrdered(OriginalCc, LastStreamSeq, newDeliverSubject, actualConsumerName, null);
                ConsumerInfo ci = Js.CreateConsumerInternal(Stream, userCc, ConsumerCreateRequestAction.Create);
                if (Sub is JetStreamAbstractSyncSubscription syncSub)
                {
                    syncSub.SetConsumerName(ci.Name);
                }
                else if (Sub is JetStreamAbstractAsyncSubscription asyncSub)
                {
                    asyncSub.SetConsumerName(ci.Name);
                }

                // 4. restart the manager.
                Startup(Sub);
            }
            catch (Exception e)
            {
                NATSException ne = e is NATSException ? (NATSException)e : new NATSException("Ordered Subscription Error", e);
                ((Connection)Js.Conn).ScheduleErrorEvent(this, ne);
                InitOrResetHeartbeatTimer();
            }
        }
    }
}
