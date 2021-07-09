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

        private MsgHeader MergeNum(MsgHeader h, String key, long value) {
            if (value > 0) {
                if (h == null) {
                    h = new MsgHeader(h);
                }
                h.Set(key, value.ToString());
            }
            return h;
        }

        private MsgHeader MergeString(MsgHeader h, String key, String value) {
            if (!string.IsNullOrWhiteSpace(value)) {
                if (h == null) {
                    h = new MsgHeader(h);
                }
                h.Set(key, value);
            }
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
            return ProcessPublishResponse(Connection.Request(msg), options);
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
        {
            throw new NotImplementedException();
        }

        public Task<PublishAck> PublishAsync(string subject, byte[] data, PublishOptions publishOptions)
        {
            throw new NotImplementedException();
        }

        public Task<PublishAck> PublishAsync(Msg message)
        {
            throw new NotImplementedException();
        }

        public Task<PublishAck> PublishAsync(Msg message, PublishOptions publishOptions)
        {
            throw new NotImplementedException();
        }

        public IJetStreamPullSubscription SubscribePull(string subject)
        {
            Validator.ValidateSubject(subject, true);
            throw new NotImplementedException();
        }

        public IJetStreamPullSubscription SubscribePull(string subject, PullSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, EventHandler<MsgHandlerEventArgs> handler)
        {
            Validator.ValidateSubject(subject, true);
            Validator.ValidateNotNull(handler, nameof(handler));
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, EventHandler<MsgHandlerEventArgs> handler, PullSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            Validator.ValidateNotNull(handler, nameof(handler));
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler)
        {
            Validator.ValidateSubject(subject, true);
            queue = Validator.ValidateQueueName(queue, false);
            Validator.ValidateNotNull(handler, nameof(handler));
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, EventHandler<MsgHandlerEventArgs> handler, PushSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            Validator.ValidateNotNull(handler, nameof(handler));
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler, PushSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            queue = Validator.ValidateQueueName(queue, false);
            Validator.ValidateNotNull(handler, nameof(handler));
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SubscribeSync(string subject)
        {
            Validator.ValidateSubject(subject, true);
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SubscribeSync(string subject, PushSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SubscribeSync(string subject, string queue)
        {
            Validator.ValidateSubject(subject, true);
            queue = Validator.ValidateQueueName(queue, false);
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SubscribeSync(string subject, string queue, PushSubscribeOptions options)
        {
            Validator.ValidateSubject(subject, true);
            queue = Validator.ValidateQueueName(queue, false);
            throw new NotImplementedException();
        }
    }
}
