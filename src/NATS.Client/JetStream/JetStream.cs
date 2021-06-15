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
    public class JetStream : IJetStream
    {
        public string Prefix { get; }
        public JetStreamOptions Options { get; }
        public IConnection Connection { get; }
        public int Timeout { get; }

        private static readonly PublishOptions defaultPubOpts = PublishOptions.Builder().Build();

        internal JetStream(IConnection connection, JetStreamOptions options)
        {
            Connection = connection;
            Options = (options != null) ? options : JetStreamOptions.Builder().Build();
            Prefix = Options.Prefix;
            Timeout = (int)Options.RequestTimeout.Millis;

            CheckJetStream();
        }

        private string AddPrefix(string subject) => Prefix + subject;

        private Msg JSRequest(String subject, byte[] bytes, int timeout)
        {
            return Connection.Request(AddPrefix(subject), bytes, timeout);
        }

        public void CheckJetStream()
        {
            try
            {
                Msg m = JSRequest(JetStreamConstants.JsapiAccountInfo, null, Timeout);
                var s = new AccountStatistics(m);
                if (s.ErrorCode == 503)
                {
                    throw new NATSJetStreamException(s.ErrorDescription);
                }
            }
            catch (NATSNoRespondersException nre)
            {
                throw new NATSJetStreamException("JetStream is not available.", nre);
            }
            catch (NATSTimeoutException te)
            {
                throw new NATSJetStreamException("JetStream did not respond.", te);
            }
            catch (Exception e)
            {
                throw new NATSJetStreamException("An exception occurred communicating with JetStream.", e);
            }
        }

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

        private PublishAck PublishSync(string subject, byte[] data, PublishOptions opts)
            => PublishSync(new Msg(subject, null, MergeHeaders(null, opts), data), opts);


        private PublishAck PublishSync(Msg msg, PublishOptions opts)
        {
            if (msg.HasHeaders)
            {
                MergeHeaders(msg.header, opts);
            }
            else
            {
                msg.header = MergeHeaders(null, opts);
            }

            return new PublishAck(Connection.Request(msg));
        }

        public PublishAck Publish(string subject, byte[] data) => PublishSync(subject, data, null);

        public PublishAck Publish(string subject, byte[] data, PublishOptions options) => PublishSync(subject, data, options);

        PublishAck IJetStream.Publish(Msg message)
        {
            return PublishSync(message, defaultPubOpts);
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
