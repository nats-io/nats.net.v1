﻿// Copyright 2021 The NATS Authors
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
using System.Threading.Tasks;
using NATS.Client.Internals;
using static NATS.Client.Connection;

namespace NATS.Client.JetStream
{
    public class JetStream : JetStreamBase, IJetStream
    {
        internal JetStream(IConnection connection, JetStreamOptions options) : base(connection, options) {}

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

        private MsgHeader MergeNum(MsgHeader h, String key, ulong value)
        {
            return value > 0 ? _MergeString(h, key, value.ToString()) : h;
        }

        private MsgHeader MergeString(MsgHeader h, String key, String value) 
        {
            return string.IsNullOrWhiteSpace(value) ? h : _MergeString(h, key, value);
        }

        private MsgHeader _MergeString(MsgHeader h, String key, String value) 
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
            String ackStream = ack.Stream;
            String pubStream = options?.Stream;
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
            
            return ProcessPublishResponse(Conn.Request(msg), options);
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

        public PublishAck Publish(string subject, byte[] data, PublishOptions options) 
            => PublishSyncInternal(subject, data, null, options);

        public PublishAck Publish(Msg msg)
            => PublishSyncInternal(msg.Subject, msg.Data, msg.Header, null);

        public PublishAck Publish(Msg msg, PublishOptions publishOptions)
            => PublishSyncInternal(msg.Subject, msg.Data, msg.Header, publishOptions);

        public Task<PublishAck> PublishAsync(string subject, byte[] data)
            => PublishAsyncInternal(subject, data, null, null);

        public Task<PublishAck> PublishAsync(string subject, byte[] data, PublishOptions publishOptions)
            => PublishAsyncInternal(subject, data, null, publishOptions);

        public Task<PublishAck> PublishAsync(Msg msg)
            => PublishAsyncInternal(msg.Subject, msg.Data, msg.Header, null);

        public Task<PublishAck> PublishAsync(Msg msg, PublishOptions publishOptions)
            => PublishAsyncInternal(msg.Subject, msg.Data, msg.Header, publishOptions);
        
        // ----------------------------------------------------------------------------------------------------
        // Subscribe
        // ----------------------------------------------------------------------------------------------------
        Subscription CreateSubscription(string subject, string queueName,
            EventHandler<MsgHandlerEventArgs> handler, bool autoAck,
            PushSubscribeOptions pushOpts, 
            PullSubscribeOptions pullOpts)
        {
            // first things first...
            bool isPullMode = pullOpts != null;

            // setup the configuration, use a default.
            string stream;
            ConsumerConfiguration.ConsumerConfigurationBuilder ccBuilder;
            SubscribeOptions so;

            if (isPullMode) {
                so = pullOpts; // options must have already been checked to be non null
                stream = pullOpts.Stream;
                ccBuilder = ConsumerConfiguration.Builder(pullOpts.ConsumerConfiguration);
                ccBuilder.WithDeliverSubject(null); // pull mode can't have a deliver subject
                // queueName is already null
                ccBuilder.WithDeliverGroup(null);   // pull mode can't have a deliver group
            }
            else {
                so = pushOpts ?? PushSubscribeOptions.Builder().Build();
                stream = so.Stream; // might be null, that's ok (see direct)
                ccBuilder = ConsumerConfiguration.Builder(so.ConsumerConfiguration);
                ccBuilder.WithMaxPullWaiting(0); // this does not apply to push, in fact will error b/c deliver subject will be set
                // deliver subject does not have to be cleared
                // figure out the queue name
                queueName = Validator.ValidateMustMatchIfBothSupplied(ccBuilder.DeliverGroup, queueName,
                    "[SUB-Q01] Consumer Configuration DeliverGroup", "Queue Name");
                ccBuilder.WithDeliverGroup(queueName); // and set it in case the deliver group was null
            }

            //
            bool bindMode = so.Bind;

            string durable = ccBuilder.Durable;
            string inboxDeliver = ccBuilder.DeliverSubject;
            string filterSubject = ccBuilder.FilterSubject;

            bool createConsumer = true;

            // 1. Did they tell me what stream? No? look it up.
            // subscribe options will have already validated that stream is present for direct mode
            if (string.IsNullOrWhiteSpace(stream)) {
                stream = LookupStreamBySubject(subject);
            }
            
            // 2. Is this a durable or ephemeral
            if (!string.IsNullOrWhiteSpace(durable)) {
                ConsumerInfo lookedUpInfo = 
                    LookupConsumerInfo(stream, durable);

                if (lookedUpInfo != null) { // the consumer for that durable already exists
                    createConsumer = false;
                    ConsumerConfiguration lookedUpConfig = lookedUpInfo.Configuration;

                    string lookedUp = lookedUpConfig.DeliverSubject;
                    if (isPullMode) {
                        if (!string.IsNullOrWhiteSpace(lookedUp)) {
                            throw new ArgumentException($"[SUB-DS01] Consumer is already configured as a push consumer with deliver subject '{lookedUp}'.");
                        }
                    }
                    else if (string.IsNullOrWhiteSpace(lookedUp)) {
                        throw new ArgumentException("[SUB-DS02] Consumer is already configured as a pull consumer with no deliver subject.");
                    }
                    else if (!string.IsNullOrWhiteSpace(inboxDeliver) && inboxDeliver != lookedUp) {
                        throw new ArgumentException($"[SUB-DS03] Existing consumer deliver subject '{lookedUp}' does not match requested deliver subject '{inboxDeliver}'.");
                    }

                    // durable already exists, make sure the filter subject matches
                    lookedUp = Validator.EmptyAsNull(lookedUpConfig.FilterSubject);
                    if (!string.IsNullOrWhiteSpace(filterSubject) && !filterSubject.Equals(lookedUp)) {
                        throw new ArgumentException(
                            $"[SUB-FS01] Subject {subject} mismatches consumer configuration {filterSubject}.");
                    }
                    filterSubject = lookedUp;

                    lookedUp = Validator.EmptyAsNull(lookedUpConfig.DeliverGroup);
                    if (string.IsNullOrWhiteSpace(lookedUp)) {
                        // lookedUp was null, means existing consumer is not a queue consumer
                        if (string.IsNullOrWhiteSpace(queueName)) {
                            // ok fine, no queue requested and the existing consumer is also not a queue consumer
                            // we must check if the consumer is in use though
                            if (lookedUpInfo.PushBound) {
                                throw new ArgumentException($"[SUB-Q02] Consumer [{durable}] is already bound to a subscription.");
                            }
                        }
                        else { // else they requested a queue but this durable was not configured as queue
                            throw new ArgumentException($"[SUB-Q03] Existing consumer [{durable}] is not configured as a queue / deliver group.");
                        }
                    }
                    else if (string.IsNullOrWhiteSpace(queueName)) {
                        throw new ArgumentException($"[SUB-Q04] Existing consumer [{durable}] is configured as a queue / deliver group.");
                    }
                    else if (lookedUp != queueName) {
                        throw new ArgumentException(
                            $"[SUB-Q05] Existing consumer deliver group {lookedUp} does not match requested queue / deliver group {queueName}.");
                    }
                    
                    inboxDeliver = lookedUpConfig.DeliverSubject; // use the deliver subject as the inbox. It may be null, that's ok
                }
                else if (bindMode) {
                    throw new ArgumentException("[SUB-B01] Consumer not found for durable. Required in bind mode.");
                }
            }

            // 3. If no deliver subject (inbox) provided or found, make an inbox.
            if (string.IsNullOrWhiteSpace(inboxDeliver)) {
                inboxDeliver = Conn.NewInbox();
            }

            // 4. create the subscription
            Subscription sub;
            if (isPullMode)
            {
                sub = ((Connection) Conn).subscribeSync(inboxDeliver, queueName, PullSubDelegate);
            }
            else if (handler == null) {
                sub = ((Connection) Conn).subscribeSync(inboxDeliver, queueName, PushSyncSubDelegate);
            }
            else if (autoAck)
            {
                void AutoAckHandler(object sender, MsgHandlerEventArgs args)
                {
                    try
                    {
                        handler.Invoke(sender, args);
                        if (args.Message.IsJetStream)
                        {
                            args.Message.Ack();
                        } 
                    }
                    catch (Exception)
                    {
                        // todo send to error listener
                    }
                }

                sub = ((Connection) Conn).subscribeAsync(inboxDeliver, queueName, AutoAckHandler, PushAsyncSubDelegate);
            }
            else {
                sub = ((Connection) Conn).subscribeAsync(inboxDeliver, queueName, handler, PushAsyncSubDelegate);
            }

            // 5-Consumer didn't exist. It's either ephemeral or a durable that didn't already exist.
            if (createConsumer) {
                // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
                if (!isPullMode) {
                    ccBuilder.WithDeliverSubject(inboxDeliver);
                }

                // being discussed if this is correct, but leave it for now.
                ccBuilder.WithFilterSubject(string.IsNullOrWhiteSpace(filterSubject) ? subject : filterSubject);

                // createOrUpdateConsumer can fail for security reasons, maybe other reasons?
                ConsumerInfo ci;
                try {
                    ci = AddOrUpdateConsumerInternal(stream, ccBuilder.Build());
                } 
                catch (NATSJetStreamException)
                {
                    sub.Unsubscribe();
                    throw;
                }
                ((IJetStreamSubscriptionInternal)sub).SetupJetStream(this, ci.Name, ci.Stream, inboxDeliver);
            }
            // 5-Consumer did exist.
            else {
                ((IJetStreamSubscriptionInternal)sub).SetupJetStream(this, durable, stream, inboxDeliver);
            }

            return sub;
        }

        private CreateSyncSubscriptionDelegate PullSubDelegate =
            (conn, subject, queue) => new JetStreamPullSubscription(conn, subject, queue);

        private CreateSyncSubscriptionDelegate PushSyncSubDelegate =
            (conn, subject, queue) => new JetStreamPushSyncSubscription(conn, subject, queue);

        private CreateAsyncSubscriptionDelegate PushAsyncSubDelegate =
            (conn, subject, queue) => new JetStreamPushAsyncSubscription(conn, subject, queue);
            
        internal ConsumerInfo LookupConsumerInfo(string lookupStream, string lookupConsumer) {
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

        internal string LookupStreamBySubject(string subject)
        {
            byte[] body = JsonUtils.SimpleMessageBody(ApiConstants.Subject, subject); 
            Msg resp = RequestResponseRequired(JetStreamConstants.JsapiStreamNames, body, Timeout);
            StreamNamesReader snr = new StreamNamesReader();
            snr.Process(resp);
            if (snr.Strings.Count != 1) {
                throw new NATSJetStreamException($"No matching streams for subject: {subject}");
            }

            return snr.Strings[0];
        }

        public IJetStreamPullSubscription PullSubscribe(string subject, PullSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            Validator.ValidateNotNull(options, "PullSubscribeOptions");
            return (IJetStreamPullSubscription) CreateSubscription(subject, null, null, false, null, options);
        }

        public IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, EventHandler<MsgHandlerEventArgs> handler, bool autoAck)
        {
            Validator.ValidateSubject(subject, true);
            Validator.ValidateNotNull(handler, nameof(handler));
            return (IJetStreamPushAsyncSubscription) CreateSubscription(subject, null, handler, autoAck, null, null);
        }

        public IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler, bool autoAck)
        {
            Validator.ValidateSubject(subject, true);
            queue = Validator.EmptyAsNull(Validator.ValidateQueueName(queue, false));
            Validator.ValidateNotNull(handler, nameof(handler));
            return (IJetStreamPushAsyncSubscription) CreateSubscription(subject, queue, handler, autoAck, null, null);
        }

        public IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, EventHandler<MsgHandlerEventArgs> handler, bool autoAck, PushSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            Validator.ValidateNotNull(handler, nameof(handler));
            return (IJetStreamPushAsyncSubscription) CreateSubscription(subject, null, handler, autoAck, options, null);
        }

        public IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler, bool autoAck, PushSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            queue = Validator.EmptyAsNull(Validator.ValidateQueueName(queue, false));
            Validator.ValidateNotNull(handler, nameof(handler));
            return (IJetStreamPushAsyncSubscription) CreateSubscription(subject, queue, handler, autoAck, options, null);
        }

        public IJetStreamPushSyncSubscription PushSubscribeSync(string subject)
        {
            Validator.ValidateSubject(subject, true);
            return (IJetStreamPushSyncSubscription) CreateSubscription(subject, null, null, false, null, null);
        }

        public IJetStreamPushSyncSubscription PushSubscribeSync(string subject, PushSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            return (IJetStreamPushSyncSubscription) CreateSubscription(subject, null, null, false, options, null);
        }

        public IJetStreamPushSyncSubscription PushSubscribeSync(string subject, string queue)
        {
            Validator.ValidateSubject(subject, true);
            queue = Validator.EmptyAsNull(Validator.ValidateQueueName(queue, false));
            return (IJetStreamPushSyncSubscription) CreateSubscription(subject, queue, null, false, null, null);
        }

        public IJetStreamPushSyncSubscription PushSubscribeSync(string subject, string queue, PushSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            queue = Validator.EmptyAsNull(Validator.ValidateQueueName(queue, false));
            return (IJetStreamPushSyncSubscription) CreateSubscription(subject, queue, null, false, options, null);
        }
    }
}
