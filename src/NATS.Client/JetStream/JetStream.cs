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

namespace NATS.Client.JetStream
{
    public class JetStream : IJetStream
    {
        public string Prefix { get; }
        public JetStreamOptions Options { get; }
        public IConnection Connection { get; }

        private readonly int timeout;

        internal JetStream(IConnection connection, JetStreamOptions options)
        {
            Prefix = options.Prefix;
            Connection = connection;
            Options = options;
            timeout = (int)options.RequestTimeout.Millis;
        }

        internal JetStream(IConnection connection) : this(connection, JetStreamOptions.Builder().Build()) { }

        // Build the headers.  Take care not to unnecessarily allocate, we're
        // in the fastpath here.
        private MsgHeader MergeHeaders(MsgHeader headers, PublishOptions opts)
        {
            if (opts == null)
                return null;

            MsgHeader mh = headers != null ? headers : new MsgHeader();

            if (!string.IsNullOrEmpty(opts.ExpectedLastMsgId))
            {
                if (mh == null) mh = new MsgHeader();
                mh.Set(MsgHeader.ExpLastIdHeader, opts.ExpectedLastMsgId);
            }
            if (opts.ExpectedLastSeq >= 0)
            {
                if (mh == null) mh = new MsgHeader();
                mh.Set(MsgHeader.ExpLastSeqHeader, opts.ExpectedLastSeq.ToString());
            }
            if (!string.IsNullOrEmpty(opts.ExpectedStream))
            {
                if (mh == null) mh = new MsgHeader();
                mh.Set(MsgHeader.ExpStreamHeader, opts.ExpectedStream);
            }
            if (!string.IsNullOrEmpty(opts.MessageId))
            {
                if (mh == null) mh = new MsgHeader();
                mh.Set(MsgHeader.MsgIdHeader, opts.MessageId);
            }

            return mh;
        }

        MsgHeader ProcessOpts(MsgHeader mh, PublishOptions opts)
        {

        }

        private PublishAck PublishSync(string subject, byte[] data, PublishOptions opts)
        {
            Msg response;

            MsgHeader mh = MergeHeaders(null, opts);
            if (mh != null)
            {
                // TODO:  use internal connection API to avoid creating message
                response = Connection.Request(new Msg(subject, null, mh, data));
            }
            else
            {
                response = Connection.Request(subject, data);
            }
            return new PublishAck(response);
        }

        private PublishAck PublishSync(Msg msg, PublishOptions opts)
        {
            // TODO process options
            return new PublishAck(Connection.Request(msg));
        }

        public PublishAck Publish(string subject, byte[] data) => PublishSync(subject, data, null);

        public PublishAck Publish(string subject, byte[] data, PublishOptions options) => PublishSync(subject, data, options);

        PublishAck IJetStream.Publish(Msg message)
        {
            throw new NotImplementedException();
        }

        PublishAck IJetStream.Publish(Msg message, PublishOptions publishOptions)
        {
            throw new NotImplementedException();
        }

        Task<PublishAck> IJetStream.PublishAsync(string subject, byte[] data)
        {
            throw new NotImplementedException();
        }

        Task<PublishAck> IJetStream.PublishAsync(string subject, byte[] data, PublishOptions publishOptions)
        {
            throw new NotImplementedException();
        }

        Task<PublishAck> IJetStream.PublishAsync(Msg message)
        {
            throw new NotImplementedException();
        }

        Task<PublishAck> IJetStream.PublishAsync(Msg message, PublishOptions publishOptions)
        {
            throw new NotImplementedException();
        }
        public IJetStreamPullSubscription PullSubscribe(string subject, SubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, EventHandler<MsgHandlerEventArgs> handler, PullSubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SyncSubsribe(string subject, SubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        public IJetStreamPullSubscription PullSubscribe(string subject)
        {
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, EventHandler<MsgHandlerEventArgs> handler)
        {
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler)
        {
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler, PullSubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SyncSubsribe(string subject)
        {
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SyncSubsribe(string subject, string queue)
        {
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SyncSubsribe(string subject, string queue, SubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        IJetStreamPullSubscription IJetStream.SubscribePull(string subject)
        {
            throw new NotImplementedException();
        }

        IJetStreamPullSubscription IJetStream.SubscribePull(string subject, PullSubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        IJetStreamPushSubscription IJetStream.Subscribe(string subject, EventHandler<MsgHandlerEventArgs> handler, PushSubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        IJetStreamPushSubscription IJetStream.Subscribe(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler, PushSubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        IJetStreamSyncSubscription IJetStream.SubscribeSync(string subject)
        {
            throw new NotImplementedException();
        }

        IJetStreamSyncSubscription IJetStream.SubscribeSync(string subject, SubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        IJetStreamSyncSubscription IJetStream.SubscribeSync(string subject, string queue)
        {
            throw new NotImplementedException();
        }

        IJetStreamSyncSubscription IJetStream.SubscribeSync(string subject, string queue, SubscribeOptions options)
        {
            throw new NotImplementedException();
        }
    }
}
