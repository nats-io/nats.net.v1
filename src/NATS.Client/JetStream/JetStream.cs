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
using System.Text;
using System.Threading.Tasks;
using NATS.Client.Internals;
using static NATS.Client.Connection;
using static NATS.Client.JetStream.ConsumerConfiguration;

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
            ConsumerConfiguration userCC; // close as we are going to get to what the user defaulted or supplied
            ConsumerConfigurationBuilder ccBuilder;
            SubscribeOptions so;

            if (isPullMode) {
                so = pullOpts; // options must have already been checked to be non null
                stream = pullOpts.Stream;

                ccBuilder = Builder(pullOpts.ConsumerConfiguration);
                userCC = ccBuilder.Build();
                
                queueName = null; // should already be, just make sure
                ccBuilder.WithDeliverSubject(null); // pull mode can't have a deliver subject
                ccBuilder.WithDeliverGroup(null);   // pull mode can't have a deliver group
            }
            else {
                so = pushOpts ?? PushSubscribeOptions.Builder().Build();
                stream = so.Stream; // might be null, that's ok (see direct)
                
                ccBuilder = Builder(so.ConsumerConfiguration);

                // figure out the queue name
                queueName = Validator.ValidateMustMatchIfBothSupplied(ccBuilder.DeliverGroup, queueName,
                    "[SUB-Q01] Consumer Configuration DeliverGroup", "Queue Name");
                
                // deliver subject does not have to be cleared, and set it in case the deliver group was null
                ccBuilder.WithDeliverGroup(queueName); 

                // did queue stuff before setting userCC because user can provide only queue
                // and we make it deliverGroup normally below, so do the same here
                userCC = ccBuilder.Build();

                ccBuilder.WithMaxPullWaiting(0); // this does not apply to push, in fact will error b/c deliver subject will be set
            }

            bool bindMode = so.Bind;

            ConsumerConfiguration consumerConfig = null;
            string consumerName = ccBuilder.Durable;
            string inboxDeliver = ccBuilder.DeliverSubject;

            // 1. Did they tell me what stream? No? look it up.
            // subscribe options will have already validated that stream is present for direct mode
            if (string.IsNullOrWhiteSpace(stream)) {
                stream = LookupStreamBySubject(subject);
            }
            
            // 2. Is this a durable or ephemeral
            if (!string.IsNullOrWhiteSpace(consumerName)) {
                ConsumerInfo lookedUpInfo = LookupConsumerInfo(stream, consumerName);

                if (lookedUpInfo != null) { // the consumer for that durable already exists
                    consumerConfig = lookedUpInfo.Configuration;

                    string lookedUp = consumerConfig.DeliverSubject;
                    if (isPullMode) {
                        if (!string.IsNullOrWhiteSpace(lookedUp)) {
                            throw new ArgumentException(
                                $"[SUB-DS01] Consumer is already configured as a push consumer with deliver subject '{lookedUp}'.");
                        }
                    }
                    else if (string.IsNullOrWhiteSpace(lookedUp)) {
                        throw new ArgumentException("[SUB-DS02] Consumer is already configured as a pull consumer with no deliver subject.");
                    }
                    else if (!string.IsNullOrWhiteSpace(inboxDeliver) && inboxDeliver != lookedUp) {
                        throw new ArgumentException($"[SUB-DS03] Existing consumer deliver subject '{lookedUp}' does not match requested deliver subject '{inboxDeliver}'.");
                    }

                    // durable already exists, make sure the filter subject matches
                    lookedUp = Validator.EmptyAsNull(consumerConfig.FilterSubject);
                    string userFilterSubject = ccBuilder.FilterSubject;
                    if (!string.IsNullOrWhiteSpace(userFilterSubject) && !userFilterSubject.Equals(lookedUp)) {
                        throw new ArgumentException(
                            $"[SUB-FS01] Subject {subject} mismatches consumer configuration {userFilterSubject}.");
                    }

                    lookedUp = Validator.EmptyAsNull(consumerConfig.DeliverGroup);
                    if (string.IsNullOrWhiteSpace(lookedUp)) {
                        // lookedUp was null, means existing consumer is not a queue consumer
                        if (string.IsNullOrWhiteSpace(queueName)) {
                            // ok fine, no queue requested and the existing consumer is also not a queue consumer
                            // we must check if the consumer is in use though
                            if (lookedUpInfo.PushBound) {
                                throw new ArgumentException($"[SUB-PB01] Consumer [{consumerName}] is already bound to a subscription.");
                            }
                        }
                        else { // else they requested a queue but this durable was not configured as queue
                            throw new ArgumentException($"[SUB-Q01] Existing consumer [{consumerName}] is not configured as a queue / deliver group.");
                        }
                    }
                    else if (string.IsNullOrWhiteSpace(queueName)) {
                        throw new ArgumentException($"[SUB-Q02] Existing consumer [{consumerName}] is configured as a queue / deliver group.");
                    }
                    else if (lookedUp != queueName) {
                        throw new ArgumentException(
                            $"[SUB-Q03] Existing consumer deliver group {lookedUp} does not match requested queue / deliver group {queueName}.");
                    }
                    
                    // check to see if the user sent a different version than the server has
                    // modifications are not allowed
                    // previous checks for deliver subject and filter subject matching are now
                    // in the changes function
                    string changes = UserVersusServer(userCC, consumerConfig);
                    if (changes != null) {
                        throw new ArgumentException($"[SUB-CC01] Existing consumer cannot be modified. {changes}");
                    }

                    inboxDeliver = consumerConfig.DeliverSubject; // use the deliver subject as the inbox. It may be null, that's ok
                }
                else if (bindMode) {
                    throw new ArgumentException("[SUB-BND01] Consumer not found for durable. Required in bind mode.");
                }
            }

            // 3. If no deliver subject (inbox) provided or found, make an inbox.
            if (string.IsNullOrWhiteSpace(inboxDeliver)) {
                inboxDeliver = Conn.NewInbox();
            }

            // 4. If consumer does not exist, create
            if (consumerConfig == null) {
                // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
                if (!isPullMode) {
                    ccBuilder.WithDeliverSubject(inboxDeliver);
                }

                // being discussed if this is correct, but leave it for now.
                string userFilterSubject = ccBuilder.FilterSubject;
                ccBuilder.WithFilterSubject(string.IsNullOrWhiteSpace(userFilterSubject) ? subject : userFilterSubject);

                // createOrUpdateConsumer can fail for security reasons, maybe other reasons?
                ConsumerInfo ci = AddOrUpdateConsumerInternal(stream, ccBuilder.Build());
                consumerName = ci.Name;
                consumerConfig = ci.Configuration;
            }

            // 5. Queue Mode Check
            bool queueMode = string.IsNullOrWhiteSpace(queueName);
            if (queueMode && (consumerConfig.FlowControl || consumerConfig.IdleHeartbeat.Millis > 0)) {
                throw new ArgumentException("[SUB-QM01] Cannot use queue when consumer has Flow Control or Heartbeat.");
            }

            // 6. create the subscription
            string fnlStream = stream;
            string fnlConsumerName = consumerName;
            string fnlInboxDeliver = inboxDeliver;

            JetStreamAutoStatusManager asm =
                new JetStreamAutoStatusManager((Connection) Conn, so, consumerConfig, queueName != null, handler == null);
            
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
            
            ((IJetStreamSubscriptionInternal)sub).SetupJetStream(this, consumerName, stream, inboxDeliver);

            return sub;
        }

        private CreateSyncSubscriptionDelegate PullSubDelegate =
            (conn, subject, queue) => new JetStreamPullSubscription(conn, subject, queue);

        private CreateSyncSubscriptionDelegate PushSyncSubDelegate =
            (conn, subject, queue) => new JetStreamPushSyncSubscription(conn, subject, queue);

        private CreateAsyncSubscriptionDelegate PushAsyncSubDelegate =
            (conn, subject, queue) => new JetStreamPushAsyncSubscription(conn, subject, queue);

    private static string UserVersusServer(ConsumerConfiguration user, ConsumerConfiguration server) {

        StringBuilder sb = new StringBuilder();
        Comp(sb, user.FlowControl, server.FlowControl, "Flow Control");
        Comp(sb, user.DeliverPolicy, server.DeliverPolicy, "Deliver Policy");
        Comp(sb, user.AckPolicy, server.AckPolicy, "Ack Policy");
        Comp(sb, user.ReplayPolicy, server.ReplayPolicy, "Replay Policy");

        Comp(sb, user.StartSeq, server.StartSeq, CcNumeric.StartSeq);
        Comp(sb, user.MaxDeliver, server.MaxDeliver, CcNumeric.MaxDeliver);
        Comp(sb, user.RateLimit, server.RateLimit, CcNumeric.RateLimit);
        Comp(sb, user.MaxAckPending, server.MaxAckPending, CcNumeric.MaxAckPending);
        Comp(sb, user.MaxPullWaiting, server.MaxPullWaiting, CcNumeric.MaxPullWaiting);

        Comp(sb, user.Description, server.Description, "Description");
        Comp(sb, user.StartTime, server.StartTime, "Start Time");
        Comp(sb, user.AckWait, server.AckWait, "Ack Wait");
        Comp(sb, user.SampleFrequency, server.SampleFrequency, "Sample Frequency");
        Comp(sb, user.IdleHeartbeat, server.IdleHeartbeat, "Idle Heartbeat");

        return sb.Length == 0 ? null : sb.ToString();
    }

    private static void Comp(StringBuilder sb, string requested, string retrieved, string name)
    {
        string q = string.IsNullOrEmpty(requested) ? null : requested;
        string t = string.IsNullOrEmpty(retrieved) ? null : retrieved;
        if (!Equals(q, t)) {
            AppendErr(sb, requested, retrieved, name);
        }
    }

    private static void Comp(StringBuilder sb, object requested, object retrieved, string name)
    {
        if (!Equals(requested, retrieved)) {
            AppendErr(sb, requested, retrieved, name);
        }
    }

    private static void Comp(StringBuilder sb, long requested, long retrieved, CcNumeric field) 
    {
        if (field.Comparable(requested) != field.Comparable(retrieved)) {
            AppendErr(sb, requested, retrieved, field.GetErr());
        }
    }

    private static void Comp(StringBuilder sb, ulong requested, ulong retrieved, CcNumeric field) {
        if (field.Comparable(requested) != field.Comparable(retrieved)) {
            AppendErr(sb, requested, retrieved, field.GetErr());
        }
    }

    private static void AppendErr(StringBuilder sb, Object requested, Object retrieved, string name) {
        if (sb.Length > 0) {
            sb.Append(", ");
        }
        sb.Append(name).Append(" [").Append(requested).Append(" vs. ").Append(retrieved).Append(']');
    }
            
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

        private string LookupStreamBySubject(string subject)
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
