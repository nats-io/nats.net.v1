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
            if (pubStream != null && !pubStream.Equals(ackStream)) {
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

        private Task<PublishAck> PublishAsyncInternal(string subject, byte[] data, MsgHeader hdr, PublishOptions options)
        {
            MsgHeader merged = MergePublishOptions(hdr, options);
            Msg msg = new Msg(subject, null, merged, data);

            if (JetStreamOptions.IsPublishNoAck)
            {
                Conn.Publish(msg);
                return null;
            }

            Duration timeout = options == null ? JetStreamOptions.RequestTimeout : options.StreamTimeout;

            throw new NotImplementedException("PublishAsync Not Completely Implemented");

            // -----
            // This is the attempt 1. Works sorta but won't build on CI/CD doesn't like the await
            // warning CS1998: This async method lacks 'await' operators and will run synchronously. Consider using the 'await' operator to await non-blocking API calls, or 'await Task.Run(...)' to do CPU-bound work on a background thread
            // -----
            // async Task<PublishAck> ContinuationFunction(Task<Msg> antecedent) 
            // => ProcessPublishResponse(antecedent.Result, options);
            //
            // return await Conn.RequestAsync(msg, timeout.Millis)
            // .ContinueWith(ContinuationFunction, CancellationToken.None)
            // .Unwrap();
            // -----

            // -----
            // This is the attempt 2
            // -----
            // Duration timeout = options == null ? JetStreamOptions.RequestTimeout : options.StreamTimeout;
            //
            // Task<PublishAck> paTask = new Task<PublishAck>(() =>
            // {
            //     Task<Msg> msgTask = Conn.RequestAsync(msg, timeout.Millis);
            //     msgTask.Wait();
            //     return ProcessPublishResponse(msgTask.Result, options);
            // });
            //
            // return await paTask.ConfigureAwait(false);
            // -----
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
            ConsumerConfigurationBuilder ccBuilder;
            SubscribeOptions so;

            if (isPullMode) {
                so = pullOpts;
                stream = pullOpts.Stream;
                ccBuilder = Builder(pullOpts.ConsumerConfiguration);
                ccBuilder.WithDeliverSubject(null); // pull mode can't have a deliver subject
            }
            else {
                so = pushOpts == null
                    ? PushSubscribeOptions.Builder().Build()
                    : pushOpts;
                stream = so.Stream; // might be null, that's ok (see direct)
                ccBuilder = Builder(so.ConsumerConfiguration);
            }

            //
            bool direct = so.Direct;

            string durable = ccBuilder.Durable;
            string inbox = ccBuilder.DeliverSubject;
            string filterSubject = ccBuilder.FilterSubject;

            bool createConsumer = true;

            // 1. Did they tell me what stream? No? look it up.
            // subscribe options will have already validated that stream is present for direct mode
            if (stream == null) {
                stream = LookupStreamBySubject(subject);
            }
            
            // 2. Is this a durable or ephemeral
            if (durable != null) {
                ConsumerInfo consumerInfo = 
                    LookupConsumerInfo(stream, durable);

                if (consumerInfo != null) { // the consumer for that durable already exists
                    createConsumer = false;
                    ConsumerConfiguration cc = consumerInfo.Configuration;

                    // durable already exists, make sure the filter subject matches
                    string existingFilterSubject = cc.FilterSubject;
                    if (filterSubject != null && !filterSubject.Equals(existingFilterSubject)) {
                        throw new ArgumentException(
                            $"Subject {subject} mismatches consumer configuration {filterSubject}.");
                    }

                    filterSubject = existingFilterSubject;

                    // use the deliver subject as the inbox. It may be null, that's ok
                    inbox = cc.DeliverSubject;
                }
                else if (direct) {
                    throw new ArgumentException("Consumer not found for durable. Required in direct mode.");
                }
            }

            // 3. If no deliver subject (inbox) provided or found, make an inbox.
            if (inbox == null) {
                inbox = Conn.NewInbox();
            }

            // 4. create the subscription
            Subscription sub;
            if (isPullMode)
            {
                sub = ((Connection) Conn).subscribeSync(inbox, queueName, PullSubDelegate);
            }
            else if (handler == null) {
                sub = ((Connection) Conn).subscribeSync(inbox, queueName, PushSyncSubDelegate);
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
                    catch (Exception e)
                    {
                        // todo send to error listener
                    }
                }

                sub = ((Connection) Conn).subscribeAsync(inbox, queueName, AutoAckHandler, PushAsyncSubDelegate);
            }
            else {
                sub = ((Connection) Conn).subscribeAsync(inbox, queueName, handler, PushAsyncSubDelegate);
            }

            // 5-Consumer didn't exist. It's either ephemeral or a durable that didn't already exist.
            if (createConsumer) {
                // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
                if (!isPullMode) {
                    ccBuilder.WithDeliverSubject(inbox);
                }

                // being discussed if this is correct, but leave it for now.
                ccBuilder.WithFilterSubject(filterSubject == null ? subject : filterSubject);

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
                ((IJetStreamSubscriptionInternal)sub).SetupJetStream(this, ci.Name, ci.Stream, inbox);
            }
            // 5-Consumer did exist.
            else {
                ((IJetStreamSubscriptionInternal)sub).SetupJetStream(this, durable, stream, inbox);
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

        public IJetStreamPullSubscription PullSubscribe(string subject)
        {
            Validator.ValidateSubject(subject, true);
            return (IJetStreamPullSubscription) CreateSubscription(subject, null, null, false, null, null);
        }

        public IJetStreamPullSubscription PullSubscribe(string subject, PullSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
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
            queue = Validator.ValidateQueueName(queue, false);
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
            queue = Validator.ValidateQueueName(queue, false);
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
            queue = Validator.ValidateQueueName(queue, false);
            return (IJetStreamPushSyncSubscription) CreateSubscription(subject, queue, null, false, null, null);
        }

        public IJetStreamPushSyncSubscription PushSubscribeSync(string subject, string queue, PushSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            queue = Validator.ValidateQueueName(queue, false);
            return (IJetStreamPushSyncSubscription) CreateSubscription(subject, queue, null, false, options, null);
        }
    }
}
