// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NATS.Client.Internals;
using static NATS.Client.ClientExDetail;
using static NATS.Client.Internals.Validator;

namespace NATS.Client.JetStream
{
    public class JetStream : JetStreamBase, IJetStream
    {
        protected internal JetStream(IConnection connection, JetStreamOptions options) : base(connection, options) {}

        private MsgHeader MergePublishOptions(MsgHeader headers, PublishOptions opts)
        {
            // never touch the user's original headers
            MsgHeader merged = headers == null ? null : new MsgHeader(headers);

            if (opts != null)
            {
                merged = MergeNum(merged, JetStreamConstants.ExpLastSeqHeader, opts.ExpectedLastSeq);
                merged = MergeNum(merged, JetStreamConstants.ExpLastSubjectSeqHeader, opts.ExpectedLastSubjectSeq);
                merged = MergeString(merged, JetStreamConstants.ExpLastIdHeader, opts.ExpectedLastMsgId);
                merged = MergeString(merged, JetStreamConstants.ExpStreamHeader, opts.ExpectedStream);
                merged = MergeString(merged, JetStreamConstants.MsgIdHeader, opts.MessageId);
            }

            return merged;
        }

        private MsgHeader MergeNum(MsgHeader h, string key, ulong value)
        {
            return value > 0 ? _MergeString(h, key, value.ToString()) : h;
        }

        private MsgHeader MergeString(MsgHeader h, string key, string value) 
        {
            return string.IsNullOrWhiteSpace(value) ? h : _MergeString(h, key, value);
        }

        private MsgHeader _MergeString(MsgHeader h, string key, string value) 
        {
            if (h == null) {
                h = new MsgHeader();
            }
            h.Set(key, value);
            return h;
        }

        private PublishAck ProcessPublishResponse(Msg resp, PublishOptions options) {
            if (resp.HasStatus) {
                throw new NATSJetStreamException("Error Publishing: " + resp.Status.Message);
            }

            PublishAck ack = new PublishAck(resp);
            string ackStream = ack.Stream;
            string pubStream = options?.Stream;
            // stream specified in options but different than ack should not happen but...
            if (!string.IsNullOrWhiteSpace(pubStream) && pubStream != ackStream) {
                throw new NATSJetStreamException("Expected ack from stream " + pubStream + ", received from: " + ackStream);
            }
            return ack;
        }

        private PublishAck PublishSyncInternal(string subject, byte[] data, MsgHeader hdr, PublishOptions options)
        {
            MsgHeader merged = MergePublishOptions(hdr, options);
            Msg msg = new Msg(subject, null, merged, data);

            if (JetStreamOptions.IsPublishNoAck)
            {
                Conn.Publish(msg);
                return null;
            }
            
            Duration timeout = options == null ? JetStreamOptions.RequestTimeout : options.StreamTimeout;
            
            return ProcessPublishResponse(Conn.Request(msg, timeout.Millis), options);
        }

        private async Task<PublishAck> PublishAsyncInternal(string subject, byte[] data, MsgHeader hdr, PublishOptions options)
        {
            MsgHeader merged = MergePublishOptions(hdr, options);
            Msg msg = new Msg(subject, null, merged, data);

            if (JetStreamOptions.IsPublishNoAck)
            {
                Conn.Publish(msg);
                return null;
            }

            Duration timeout = options == null ? JetStreamOptions.RequestTimeout : options.StreamTimeout;

            var result = await Conn.RequestAsync(msg, timeout.Millis).ConfigureAwait(false);
            return ProcessPublishResponse(result, options);
        }

        public PublishAck Publish(string subject, byte[] data) 
            => PublishSyncInternal(subject, data, null, null);

        public PublishAck Publish(string subject, MsgHeader headers, byte[] data) 
            => PublishSyncInternal(subject, data, headers, null);

        public PublishAck Publish(string subject, byte[] data, PublishOptions options) 
            => PublishSyncInternal(subject, data, null, options);

        public PublishAck Publish(string subject, MsgHeader headers, byte[] data, PublishOptions options) 
            => PublishSyncInternal(subject, data, headers, options);

        public PublishAck Publish(Msg msg)
            => PublishSyncInternal(msg.Subject, msg.Data, msg.Header, null);

        public PublishAck Publish(Msg msg, PublishOptions publishOptions)
            => PublishSyncInternal(msg.Subject, msg.Data, msg.Header, publishOptions);

        public Task<PublishAck> PublishAsync(string subject, byte[] data)
            => PublishAsyncInternal(subject, data, null, null);

        public Task<PublishAck> PublishAsync(string subject, MsgHeader headers, byte[] data)
            => PublishAsyncInternal(subject, data, headers, null);

        public Task<PublishAck> PublishAsync(string subject, byte[] data, PublishOptions publishOptions)
            => PublishAsyncInternal(subject, data, null, publishOptions);

        public Task<PublishAck> PublishAsync(string subject, MsgHeader headers, byte[] data, PublishOptions publishOptions)
            => PublishAsyncInternal(subject, data, headers, publishOptions);

        public Task<PublishAck> PublishAsync(Msg msg)
            => PublishAsyncInternal(msg.Subject, msg.Data, msg.Header, null);

        public Task<PublishAck> PublishAsync(Msg msg, PublishOptions publishOptions)
            => PublishAsyncInternal(msg.Subject, msg.Data, msg.Header, publishOptions);
        
        // ----------------------------------------------------------------------------------------------------
        // Subscribe
        // ----------------------------------------------------------------------------------------------------

        internal delegate MessageManager MessageManagerFactory(
            Connection conn, JetStream js, string stream, 
            SubscribeOptions so, ConsumerConfiguration cc, bool queueMode, bool syncMode);

        internal MessageManagerFactory _pushMessageManagerFactory =
            (conn, js, stream, so, cc, queueMode, syncMode) =>
                new PushMessageManager(conn, js, stream, so, cc, queueMode, syncMode);

        internal MessageManagerFactory _pushOrderedMessageManagerFactory =
            (conn, js, stream, so, cc, queueMode, syncMode) =>
                new OrderedMessageManager(conn, js, stream, so, cc, queueMode, syncMode);

        internal MessageManagerFactory _pullMessageManagerFactory =
            (conn, js, stream, so, cc, queueMode, syncMode) =>
                new PullMessageManager(conn, syncMode);
        
        Subscription CreateSubscription(string subject, string queueName,
            EventHandler<MsgHandlerEventArgs> userHandler, bool autoAck,
            PushSubscribeOptions pushSubscribeOptions, 
            PullSubscribeOptions pullSubscribeOptions)
        {
            // 1. Prepare for all the validation
            bool isPullMode = pullSubscribeOptions != null;

            SubscribeOptions so;
            string stream;
            string qgroup;
            ConsumerConfiguration userCC;

            if (isPullMode) {
                so = pullSubscribeOptions; // options must have already been checked to be non-null
                stream = pullSubscribeOptions.Stream;

                userCC = so.ConsumerConfiguration;

                qgroup = null; // just to make compiler happy both paths set variable
                ValidateNotSupplied(userCC.DeliverGroup, JsSubPullCantHaveDeliverGroup);
                ValidateNotSupplied(userCC.DeliverSubject, JsSubPullCantHaveDeliverSubject);
            }
            else {
                so = pushSubscribeOptions ?? PushSubscribeOptions.DefaultPushOpts;
                stream = so.Stream; // might be null, that's ok (see directBind)

                userCC = so.ConsumerConfiguration;

                if (!userCC.MaxPullWaiting.Equals(ConsumerConfiguration.IntUnset))
                {
                    throw JsSubPushCantHaveMaxPullWaiting.Instance();
                }
                if (!userCC.MaxBatch.Equals(ConsumerConfiguration.IntUnset))
                {
                    throw JsSubPushCantHaveMaxBatch.Instance();
                }
                if (!userCC.MaxBytes.Equals(ConsumerConfiguration.IntUnset))
                {
                    throw JsSubPushCantHaveMaxBytes.Instance();
                }

                // figure out the queue name
                qgroup = ValidateMustMatchIfBothSupplied(userCC.DeliverGroup, queueName, JsSubQueueDeliverGroupMismatch);
                if (so.Ordered && qgroup != null) {
                    throw JsSubOrderedNotAllowOnQueues.Instance();
                }
            }
            
            // 2A. Flow Control / heartbeat not always valid
            if (userCC.FlowControl || userCC.IdleHeartbeat != null && userCC.IdleHeartbeat.Millis > 0) {
                if (isPullMode) {
                    throw JsSubFcHbNotValidPull.Instance();
                }
                if (qgroup != null) {
                    throw JsSubFcHbNotValidQueue.Instance();
                }
            }

            // 2B. Did they tell me what stream? No? look it up.
            if (string.IsNullOrWhiteSpace(stream)) {
                stream = LookupStreamBySubject(subject);
                if (stream == null) {
                    throw JsSubNoMatchingStreamForSubject.Instance();
                }
            }

            ConsumerConfiguration serverCC = null;
            string consumerName = userCC.Durable;
            string inboxDeliver = userCC.DeliverSubject;
            
            // 3. Does this consumer already exist?
            if (consumerName != null) 
            {
                ConsumerInfo serverInfo = LookupConsumerInfo(stream, consumerName);

                if (serverInfo != null) { // the consumer for that durable already exists
                    serverCC = serverInfo.ConsumerConfiguration;

                    // check to see if the user sent a different version than the server has
                    // modifications are not allowed
                    IList<string> changes = userCC.GetChanges(serverCC);
                    if (changes.Count > 0) 
                    {
                        throw JsSubExistingConsumerCannotBeModified.Instance($"[{string.Join(",", changes)}]");
                    }

                    if (isPullMode) 
                    {
                        if (!string.IsNullOrWhiteSpace(serverCC.DeliverSubject)) 
                        {
                            throw JsSubConsumerAlreadyConfiguredAsPush.Instance();
                        }
                    }
                    else if (string.IsNullOrWhiteSpace(serverCC.DeliverSubject)) 
                    {
                        throw JsSubConsumerAlreadyConfiguredAsPull.Instance();
                    }

                    if (string.IsNullOrWhiteSpace(serverCC.DeliverGroup)) 
                    {
                        // lookedUp was null/empty, means existing consumer is not a queue consumer
                        if (qgroup == null) 
                        {
                            // ok fine, no queue requested and the existing consumer is also not a queue consumer
                            // we must check if the consumer is in use though
                            if (serverInfo.PushBound) 
                            {
                                throw JsSubConsumerAlreadyBound.Instance();
                            }
                        }
                        else 
                        { // else they requested a queue but this durable was not configured as queue
                            throw JsSubExistingConsumerNotQueue.Instance();
                        }
                    }
                    else if (qgroup == null) 
                    {
                        throw JsSubExistingConsumerIsQueue.Instance();
                    }
                    else if (!serverCC.DeliverGroup.Equals(qgroup)) 
                    {
                        throw JsSubExistingQueueDoesNotMatchRequestedQueue.Instance();
                    }

                    // durable already exists, make sure the filter subject matches
                    if (string.IsNullOrWhiteSpace(subject))
                    {
                        subject = userCC.FilterSubject;
                    }
                    else if (!IsFilterMatch(subject, serverCC.FilterSubject, stream))
                    {
                        throw JsSubSubjectDoesNotMatchFilter.Instance();
                    }

                    inboxDeliver = serverCC.DeliverSubject; // use the deliver subject as the inbox. It may be null, that's ok, we'll fix that later
                }
                else if (so.Bind) {
                    throw JsSubConsumerNotFoundRequiredInBind.Instance();
                }
            }

            // 4. If no deliver subject (inbox) provided or found, make an inbox.
            if (string.IsNullOrWhiteSpace(inboxDeliver)) {
                inboxDeliver = Conn.NewInbox();
            }

            // 5. If consumer does not exist, create and settle on the config. Name will have to wait
            //    If the consumer exists, I know what the settled info is
            if (serverCC == null) {
                ConsumerConfiguration.ConsumerConfigurationBuilder ccBuilder = ConsumerConfiguration.Builder(userCC);

                // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
                if (!isPullMode) {
                    ccBuilder.WithDeliverSubject(inboxDeliver);
                }

                if (string.IsNullOrWhiteSpace(userCC.FilterSubject))
                {
                    ccBuilder.WithFilterSubject(subject);
                }

                if (string.IsNullOrWhiteSpace(userCC.DeliverGroup) && !string.IsNullOrWhiteSpace(qgroup))
                {
                    ccBuilder.WithDeliverGroup(qgroup);
                }

                // createOrUpdateConsumer can fail for security reasons, maybe other reasons?
                serverCC = ccBuilder.Build();
                consumerName = null;
            }

            // 6. create the subscription
            bool syncMode = userHandler == null;
            MessageManager mm;
            Connection.CreateSyncSubscriptionDelegate syncSubDelegate = null;
            Connection.CreateAsyncSubscriptionDelegate asyncSubDelegate = null;
            if (isPullMode)
            {
                mm = _pullMessageManagerFactory.Invoke((Connection)Conn, this, stream, so, serverCC, qgroup != null, syncMode);
                syncSubDelegate = (dConn, dSubject, dQueue) =>
                {
                    return new JetStreamPullSubscription(dConn, dSubject, this, stream, consumerName, inboxDeliver, mm);
                };
            }
            else
            {
                MessageManagerFactory mmFactory = so.Ordered ? _pushOrderedMessageManagerFactory : _pushMessageManagerFactory;
                mm = mmFactory((Connection)Conn, this, stream, so, serverCC, qgroup != null, syncMode);
                
                if (syncMode)
                {
                    syncSubDelegate = (dConn, dSubject, dQueue) =>
                    {
                        JetStreamPushSyncSubscription ssub = new JetStreamPushSyncSubscription(dConn, dSubject, dQueue,
                            this, stream, consumerName, inboxDeliver, mm);
                        ssub.SetPendingLimits(so.PendingMessageLimit, so.PendingByteLimit);
                        return ssub;
                    };
                }
                else
                {
                    asyncSubDelegate = (dConn, dSubject, dQueue) =>
                    {
                        JetStreamPushAsyncSubscription asub = new JetStreamPushAsyncSubscription(dConn, dSubject,
                            dQueue, this, stream, consumerName, inboxDeliver, mm);
                        asub.SetPendingLimits(so.PendingMessageLimit, so.PendingByteLimit);
                        return asub;
                    };
                }
            }
            
            Subscription sub;
            if (syncSubDelegate != null)
            {
                sub = ((Connection)Conn).subscribeSync(inboxDeliver, queueName, syncSubDelegate); 
            }
            else
            {
                bool handlerAutoAck = autoAck && serverCC.AckPolicy != AckPolicy.None;
                EventHandler<MsgHandlerEventArgs> handler = (sender, args) => 
                {
                    if (mm.Manage(args.Message))
                    {
                        return; // manager handled the message
                    }
                            
                    userHandler.Invoke(sender, args);
                    if (handlerAutoAck)
                    {
                        args.Message.Ack();
                    }
                };
                sub = ((Connection)Conn).subscribeAsync(inboxDeliver, queueName, handler, asyncSubDelegate);
            }

            // 7. The consumer might need to be created, do it here
            if (consumerName == null)
            {
                try
                {
                    ConsumerInfo ci = AddOrUpdateConsumerInternal(stream, serverCC);
                    if (sub is JetStreamAbstractSyncSubscription syncSub)
                    {
                        syncSub.Consumer = ci.Name;
                    }
                    else if (sub is JetStreamPushAsyncSubscription asyncSub)
                    {
                        asyncSub.Consumer = ci.Name;
                    }
                }
                catch
                {
                    // create consumer can fail, unsubscribe and then throw the exception to the user
                    sub.Unsubscribe();
                    throw;
                }
            }

            return sub;
        }
        
        // protected internal so can be tested
        protected internal ConsumerInfo LookupConsumerInfo(string lookupStream, string lookupConsumer) {
            try {
                return GetConsumerInfoInternal(lookupStream, lookupConsumer);
            }
            catch (NATSJetStreamException e) {
                if (e.ApiErrorCode == JetStreamConstants.JsConsumerNotFoundErr) {
                    return null;
                }
                throw;
            }
        }

        private string LookupStreamBySubject(string subject)
        {
            IList<string> list = GetStreamNamesInternal(subject);
            return list.Count == 1 ? list[0] : null; 
        }
        
        private string LookupStreamSubject(string stream)
        {
            StreamInfo si = GetStreamInfoInternal(stream, null);
            return si.Config.Subjects.Count == 1 ? si.Config.Subjects[0] : null;
        }

        private Boolean IsFilterMatch(string subscribeSubject, string filterSubject, string stream) {

            // subscribeSubject guaranteed to not be empty or null
            // filterSubject may be null or empty or have value

            if (subscribeSubject.Equals(filterSubject)) {
                return true;
            }

            if (string.IsNullOrWhiteSpace(filterSubject) || filterSubject.Equals(">")) {
                // lookup stream subject returns null if there is not exactly one subject
                string streamSubject = LookupStreamSubject(stream);
                return subscribeSubject.Equals(streamSubject);
            }

            return false;
        }

        public IJetStreamPullSubscription PullSubscribe(string subject, PullSubscribeOptions options)
        {
            ValidateNotNull(options, "Pull Subscribe Options");
            ValidateSubject(subject, IsSubjectRequired(options));
            return (IJetStreamPullSubscription) CreateSubscription(subject, null, null, false, null, options);
        }

        public IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, EventHandler<MsgHandlerEventArgs> handler, bool autoAck)
        {
            ValidateSubject(subject, true);
            ValidateNotNull(handler, "Handler");
            return (IJetStreamPushAsyncSubscription) CreateSubscription(subject, null, handler, autoAck, null, null);
        }

        public IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler, bool autoAck)
        {
            ValidateSubject(subject, true);
            queue = EmptyAsNull(ValidateQueueName(queue, false));
            ValidateNotNull(handler, "Handler");
            return (IJetStreamPushAsyncSubscription) CreateSubscription(subject, queue, handler, autoAck, null, null);
        }

        public IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, EventHandler<MsgHandlerEventArgs> handler, bool autoAck, PushSubscribeOptions options)
        {
            ValidateSubject(subject, IsSubjectRequired(options));
            ValidateNotNull(handler, "Handler");
            return (IJetStreamPushAsyncSubscription) CreateSubscription(subject, null, handler, autoAck, options, null);
        }

        public IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler, bool autoAck, PushSubscribeOptions options)
        {
            ValidateSubject(subject, IsSubjectRequired(options));
            queue = EmptyAsNull(ValidateQueueName(queue, false));
            ValidateNotNull(handler, "Handler");
            return (IJetStreamPushAsyncSubscription) CreateSubscription(subject, queue, handler, autoAck, options, null);
        }

        public IJetStreamPushSyncSubscription PushSubscribeSync(string subject)
        {
            ValidateSubject(subject, true);
            return (IJetStreamPushSyncSubscription) CreateSubscription(subject, null, null, false, null, null);
        }

        public IJetStreamPushSyncSubscription PushSubscribeSync(string subject, PushSubscribeOptions options)
        {
            ValidateSubject(subject, IsSubjectRequired(options));
            return (IJetStreamPushSyncSubscription) CreateSubscription(subject, null, null, false, options, null);
        }

        public IJetStreamPushSyncSubscription PushSubscribeSync(string subject, string queue)
        {
            ValidateSubject(subject, true);
            queue = EmptyAsNull(ValidateQueueName(queue, false));
            return (IJetStreamPushSyncSubscription) CreateSubscription(subject, queue, null, false, null, null);
        }

        public IJetStreamPushSyncSubscription PushSubscribeSync(string subject, string queue, PushSubscribeOptions options)
        {
            ValidateSubject(subject, IsSubjectRequired(options));
            queue = EmptyAsNull(ValidateQueueName(queue, false));
            return (IJetStreamPushSyncSubscription) CreateSubscription(subject, queue, null, false, options, null);
        }

        private bool IsSubjectRequired(SubscribeOptions options) => options == null || !options.Bind;
    }
}
