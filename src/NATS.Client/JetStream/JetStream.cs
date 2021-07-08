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

namespace NATS.Client.JetStream
{
    public class JetStream : IJetStream, IJetStreamManagement
    {
        public string Prefix { get; }
        public JetStreamOptions Options { get; }
        public IConnection Connection { get; }
        public int Timeout { get; }

        private static readonly PublishOptions defaultPubOpts = PublishOptions.Builder().Build();

        internal JetStream(IConnection connection, JetStreamOptions options)
        {
            Connection = connection;
            Options = options ?? JetStreamOptions.Builder().Build();
            Prefix = Options.Prefix;
            Timeout = (int)Options.RequestTimeout.Millis;
        }

        private string AddPrefix(string subject) => Prefix + subject;

        internal Msg JSRequest(string subject, byte[] bytes, int timeout)
        {
            return Connection.Request(AddPrefix(subject), bytes, timeout);
        }

        public void CheckJetStream()
        {
            try
            {
                Msg m = JSRequest(JetStreamConstants.JsapiAccountInfo, null, Timeout);
                var s = new AccountStatistics(m);
                if (s.ErrorCode == NatsConstants.NoRespondersCode)
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

        public IJetStreamPullSubscription PullSubscribe(string subject, SubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, EventHandler<MsgHandlerEventArgs> handler, PullSubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SyncSubscribe(string subject, SubscribeOptions options)
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

        public IJetStreamSyncSubscription SyncSubscribe(string subject)
        {
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SyncSubscribe(string subject, string queue)
        {
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SyncSubscribe(string subject, string queue, SubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        public IJetStreamPullSubscription SubscribePull(string subject)
        {
            throw new NotImplementedException();
        }

        public IJetStreamPullSubscription SubscribePull(string subject, PullSubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, EventHandler<MsgHandlerEventArgs> handler, PushSubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        public IJetStreamPushSubscription Subscribe(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler, PushSubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SubscribeSync(string subject)
        {
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SubscribeSync(string subject, SubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SubscribeSync(string subject, string queue)
        {
            throw new NotImplementedException();
        }

        public IJetStreamSyncSubscription SubscribeSync(string subject, string queue, SubscribeOptions options)
        {
            throw new NotImplementedException();
        }

        #region JetStreamManagement

        public StreamInfo AddStream(StreamConfiguration config)
            => AddOrUpdateStream(config, JetStreamConstants.JsapiStreamCreate);

        public StreamInfo UpdateStream(StreamConfiguration config)
            => AddOrUpdateStream(config, JetStreamConstants.JsapiStreamUpdate);

        private StreamInfo AddOrUpdateStream(StreamConfiguration config, string template)
        {
            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            var m = JSRequest(
                string.Format(template, config.Name),
                config.Serialize(),
                Timeout);

            return new StreamInfo(new ApiResponse(m, true).JsonNode);
        }

        public bool DeleteStream(string streamName)
        {
            if (streamName == null)
            {
                throw new ArgumentNullException(nameof(streamName));
            }

            var m = JSRequest(
                string.Format(JetStreamConstants.JsapiStreamDelete, streamName),
                null,
                Timeout);

            return new SuccessApiResponse(Encoding.ASCII.GetString(m.Data)).Success;
        }

        public StreamInfo GetStreamInfo(string streamName)
        {
            if (streamName == null)
            {
                throw new ArgumentNullException(nameof(streamName));
            }

            var m = JSRequest(
                string.Format(JetStreamConstants.JsapiStreamInfo, streamName),
                null,
                Timeout);

            return new StreamInfo(new ApiResponse(m, true).JsonNode);
        }

        public PurgeResponse PurgeStream(string streamName)
        {
            if (streamName == null)
            {
                throw new ArgumentNullException(nameof(streamName));
            }

            var m = JSRequest(
                string.Format(JetStreamConstants.JsapiStreamPurge, streamName),
                null,
                Timeout);

            return new PurgeResponse(new ApiResponse(m, true).JsonNode);
        }

        public ConsumerInfo AddConsumer(string streamName, ConsumerConfiguration config)
        {
            if (streamName == null)
            {
                // TODO validate stream name
                throw new ArgumentNullException(nameof(streamName));
            }
            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            string subj;

            if (config.Durable == null)
            {
                subj = string.Format(JetStreamConstants.JsapiConsumerCreate, streamName);
            }
            else
            {
                subj = string.Format(JetStreamConstants.JsapiDurableCreate, streamName,
                    config.Durable);
            }

            var ccr = new ConsumerCreateRequest(streamName, config);
            var m = JSRequest(subj, ccr.Serialize(), Timeout);
            return new ConsumerInfo(new ApiResponse(m, true).JsonNode);
        }

        public bool DeleteConsumer(string streamName, string consumer)
        {
            if (streamName == null)
            {
                // TODO validate stream name
                throw new ArgumentNullException(nameof(streamName));
            }
            if (consumer == null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            var m = JSRequest(
                string.Format(JetStreamConstants.JsapiConsumerDelete, streamName, consumer),
                null,
                Timeout);

            return new SuccessApiResponse(Encoding.ASCII.GetString(m.Data)).Success;
        }

        public ConsumerInfo GetConsumerInfo(string streamName, string consumer)
        {
            if (streamName == null)
            {
                // TODO validate stream name
                throw new ArgumentNullException(nameof(streamName));
            }
            if (consumer == null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            var m = JSRequest(
                string.Format(JetStreamConstants.JsapiConsumerInfo, streamName, consumer),
                null,
                Timeout);

            return new ConsumerInfo(new ApiResponse(m, true).JsonNode);
        }

        public string[] GetConsumerNames(string streamName)
        {
            throw new NotImplementedException();
        }

        public ConsumerInfo[] GetConsumers(string streamName)
        {
            throw new NotImplementedException();
        }

        public string[] GetStreamNames()
        {
            throw new NotImplementedException();
        }

        public StreamInfo[] GetStreams()
        {
            throw new NotImplementedException();
        }

        public MessageInfo GetMessage(string streamName, long sequence)
        {
            throw new NotImplementedException();
        }

        public bool DeleteMessage(string streamName, long sequence)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
