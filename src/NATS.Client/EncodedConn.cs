// Copyright 2015-2018 The NATS Authors
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
using System.IO;
#if NET45
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
#endif

namespace NATS.Client
{
    /// <summary>
    /// Represents the method that will handle serialization of <paramref name="obj"/>
    /// to a byte array.
    /// </summary>
    /// <param name="obj">The <see cref="Object"/> to serialize.</param>
    public delegate byte[] Serializer(Object obj);

    /// <summary>
    /// Represents a method that will handle deserialization of a byte array
    /// into an <see cref="Object"/>.
    /// </summary>
    /// <param name="data">The byte array in a <see cref="Msg"/> payload
    /// that contains the <see cref="Object"/> to deserialize.</param>
    /// <returns>The <see cref="Object"/> being deserialized.</returns>
    public delegate Object Deserializer(byte[] data);

    /// <summary>
    /// Provides decoded messages received by subscriptions or requests.
    /// </summary>
    public class EncodedMessageEventArgs : EventArgs
    {
        internal string  subject = null;
        internal string  reply = null;
        internal object  obj = null;
        internal Msg     msg = null;

        internal EncodedMessageEventArgs() {}

        /// <summary>
        /// Gets the subject for the received <see cref="Msg"/>.
        /// </summary>
        public string Subject
        {
            get { return subject; }
        }

        /// <summary>
        /// Gets the reply topic for the received <see cref="Msg"/>.
        /// </summary>
        public string Reply
        {
            get { return reply; }
        }

        /// <summary>
        /// Gets the object decoded (deserialized) from the incoming message.
        /// </summary>
        public object ReceivedObject
        {
            get
            {
                return obj;
            }
        }

        /// <summary>
        /// Gets the original <see cref="Msg"/> that <see cref="ReceivedObject"/> was deserialized from.
        /// </summary>
        public Msg Message
        {
            get
            {
                return this.msg;
            }
        }
    }

    /// <summary>
    /// Represents an <see cref="Connection"/> which uses a client specified
    /// encoding scheme.
    /// </summary>
    public class EncodedConnection : Connection, IEncodedConnection
    {
        private MemoryStream sStream   = new MemoryStream();
        private object       sStreamLock = new object();

        private MemoryStream dStream     = new MemoryStream();
        private object       dStreamLock = new object();

        private  Serializer   onSerialize = null;
        private  Deserializer onDeserialize = null;

        internal EncodedConnection(Options opts)
            : base(opts)
        {
            onSerialize = defaultSerializer;
            onDeserialize = defaultDeserializer;
        }

#if NET45
        private IFormatter f = new BinaryFormatter();

        // Note, the connection locks around this, so while we are using a
        // byte array in the connection, we are still threadsafe.
        internal byte[] defaultSerializer(Object obj)
        {
            byte[] rv = null;

            if (obj == null)
                return null;

            lock (sStreamLock)
            {
                sStream.Position = 0;
                f.Serialize(sStream, obj);

                long len = sStream.Position;
                rv = new byte[len];

                // Could be more efficient, but w/o passing a length all the way
                // through publish, we have to do this.   No such thing as slices
                // in .NET
                Array.Copy(sStream.GetBuffer(), rv, len);
            }
            return rv;
        }


        // This could be more efficient, but is straightforward and simple.
        // it'd be nice to have a default serializer per subscription to avoid
        // the lock here, but this  mirrors go.
        //
        // TODO:  Look at moving the default to the wrapper and keeping a derialization
        // stream there.
        internal object defaultDeserializer(byte[] data)
        {
            if (data == null)
                return null;

            lock (dStreamLock)
            {
                dStream.Position = 0;
                dStream.Write(data, 0, data.Length);
                dStream.Position = 0;

                return f.Deserialize(dStream);
            }
        }
#else
        Serializer defaultSerializer = null;
        Deserializer defaultDeserializer = null;
#endif

        private void publishObject(string subject, string reply, object o)
        {
            if (onSerialize == null)
                throw new NATSException("IEncodedConnection.OnSerialize must be set (.NET core only).");

            byte[] data = onSerialize(o);
            int count = data != null ? data.Length : 0;
            publish(subject, reply, data, 0, count);
        }

        /// <summary>
        /// Publishes the serialized value of <paramref name="obj"/> to the given <paramref name="subject"/>.
        /// </summary>
        /// <param name="subject">The subject to publish <paramref name="obj"/> to over
        /// the current connection.</param>
        /// <param name="obj">The <see cref="Object"/> to serialize and publish to the connected NATS server.</param>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is 
        /// <c>null</c> or entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException">The serialzed form of <paramref name="obj"/> exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSException"><para><see cref="OnSerialize"/> is <c>null</c>.</para>
        /// <para>-or-</para>
        /// <para>There was an unexpected exception performing an internal NATS call
        /// while publishing. See <see cref="System.Exception.InnerException"/> for more details.</para></exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public void Publish(string subject, Object obj)
        {
            publishObject(subject, null, obj);
        }

        /// <summary>
        /// Publishes the serialized value of <paramref name="obj"/> to the given <paramref name="subject"/>.
        /// </summary>
        /// <param name="subject">The subject to publish <paramref name="obj"/> to over
        /// the current connection.</param>
        /// <param name="reply">An optional reply subject.</param>
        /// <param name="obj">The <see cref="Object"/> to serialize and publish to the connected NATS server.</param>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is <c>null</c> or
        /// entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException">The serialzed form of <paramref name="obj"/> exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSException"><para><see cref="OnSerialize"/> is <c>null</c>.</para>
        /// <para>-or-</para>
        /// <para>There was an unexpected exception performing an internal NATS call
        /// while publishing. See <see cref="System.Exception.InnerException"/> for more details.</para></exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public void Publish(string subject, string reply, object obj)
        {
            publishObject(subject, reply, obj);
        }

        // Wrapper the handler for keeping a local copy of the event arguments around.
        internal class EncodedHandlerWrapper
        {
            EventHandler<EncodedMessageEventArgs> mh;
            EncodedConnection c;

            internal EncodedHandlerWrapper(EncodedConnection encc, EventHandler<EncodedMessageEventArgs> handler)
            {
                mh = handler;
                c = encc;
            }

            public void msgHandlerToEncoderHandler(Object sender, MsgHandlerEventArgs args)
            {
                EncodedMessageEventArgs ehev = new EncodedMessageEventArgs();
                try
                {
                    byte[] data = args.msg != null ? args.msg.Data : null;
                    ehev.obj = c.onDeserialize(data);
                }
                catch (Exception ex)
                {
                    c.lastEx = new NATSException("Unable to deserialize message.", ex);
                    return;
                }

                ehev.subject = args.msg.Subject;
                ehev.reply = args.msg.Reply;
                ehev.msg = args.msg;

                try
                {
                    mh(this, ehev);
                }
                catch (Exception)
                {
                    /* ignore user thrown exceptions */
                }
            }
        }

        // Keep the wrappers referenced so they don't get GC'd.
        IDictionary<ISubscription, EncodedHandlerWrapper> wrappers =
            new Dictionary<ISubscription, EncodedHandlerWrapper>();

        private IAsyncSubscription subscribeAsync(string subject, string reply,
            EventHandler<EncodedMessageEventArgs> handler)
        {
            if (handler == null)
                throw new ArgumentNullException("Handler cannot be null.");

            if (onDeserialize == null)
                throw new NATSException("IEncodedConnection.OnDeserialize must be set before subscribing.");

            EncodedHandlerWrapper echWrapper = new EncodedHandlerWrapper(this, handler);

            IAsyncSubscription s = base.subscribeAsync(subject, reply,
                echWrapper.msgHandlerToEncoderHandler);

            wrappers.Add(s, echWrapper);

            return s;
        }

        /// <summary>
        /// Expresses interest in the given <paramref name="subject"/> to the NATS Server, and begins delivering
        /// messages to the given event handler.
        /// </summary>
        /// <remarks>The <see cref="IAsyncSubscription"/> returned will start delivering messages
        /// to the event handler as soon as they are received. The caller does not have to invoke
        /// <see cref="IAsyncSubscription.Start"/>.</remarks>
        /// <param name="subject">The subject on which to listen for messages.
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <param name="handler">The <see cref="EventHandler{TEventArgs}"/> invoked when messages are received 
        /// on the returned <see cref="IAsyncSubscription"/>.</param>
        /// <returns>An <see cref="IAsyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="handler"/> is <c>null</c>.</exception>
        /// <exception cref="NATSException"><see cref="OnDeserialize"/> is <c>null</c>.</exception>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is 
        /// <c>null</c> or entirely whitespace.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public IAsyncSubscription SubscribeAsync(string subject, EventHandler<EncodedMessageEventArgs> handler)
        {
            return subscribeAsync(subject, null, handler);
        }

        /// <summary>
        /// Creates an asynchronous queue subscriber on the given <paramref name="subject"/>, and begins delivering
        /// messages to the given event handler.
        /// </summary>
        /// <remarks>The <see cref="IAsyncSubscription"/> returned will start delivering messages
        /// to the event handler as soon as they are received. The caller does not have to invoke
        /// <see cref="IAsyncSubscription.Start"/>.</remarks>
        /// <param name="subject">The subject on which to listen for messages.
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <param name="queue">The name of the queue group in which to participate.</param>
        /// <param name="handler">The <see cref="EventHandler{TEventArgs}"/> invoked when messages are received 
        /// on the returned <see cref="IAsyncSubscription"/>.</param>
        /// <returns>An <see cref="IAsyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="handler"/> is <c>null</c>.</exception>
        /// <exception cref="NATSException"><see cref="OnDeserialize"/> is <c>null</c>.</exception>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is 
        /// <c>null</c> or entirely whitespace.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public IAsyncSubscription SubscribeAsync(string subject, string queue, EventHandler<EncodedMessageEventArgs> handler)
        {
            return subscribeAsync(subject, queue, handler);
        }

        // lower level method to serialize an object, send a request,
        // and deserialize the returning object.
        private object requestObject(string subject, object obj, int timeout)
        {
            byte[] data = onSerialize(obj);
            int count = data != null ? data.Length : 0;
            Msg m = base.request(subject, data, 0, count, timeout);
            return onDeserialize(m.Data);
        }

        /// <summary>
        /// Sends a request payload and returns the deserialized response, or throws
        /// <see cref="NATSTimeoutException"/> if the <paramref name="timeout"/> expires.
        /// </summary>
        /// <remarks>
        /// <see cref="Request(string, object, int)"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="IEncodedConnection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="obj"/> to over
        /// the current connection.</param>
        /// <param name="obj">The <see cref="Object"/> to serialize and publish to the connected NATS server.</param>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        /// <returns>A <see cref="Object"/> with the deserialized response from the NATS server.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is less than or equal to zero 
        /// (<c>0</c>).</exception>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is <c>null</c> or
        /// entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException">The serialzed form of <paramref name="obj"/> exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the 
        /// response.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call
        /// while executing the request. See <see cref="System.Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public object Request(string subject, object obj, int timeout)
        {
            return requestObject(subject, obj, timeout);
        }

        /// <summary>
        /// Sends a request payload and returns the deserialized response.
        /// </summary>
        /// <remarks>
        /// <see cref="Request(string, object)"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="IEncodedConnection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="obj"/> to over
        /// the current connection.</param>
        /// <param name="obj">The <see cref="Object"/> to serialize and publish to the connected NATS server.</param>
        /// <returns>A <see cref="Object"/> with the deserialized response from the NATS server.</returns>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is <c>null</c> or 
        /// entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException">The serialzed form of <paramref name="obj"/> exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the
        /// response.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while
        /// executing the request. See <see cref="System.Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public object Request(string subject, object obj)
        {
            return requestObject(subject, obj, -1);
        }

        // when our base (Connection) removes a subscriber, clean up
        // our references to the encoded handler wrapper.
        internal override void removeSub(Subscription s)
        {
            // The subscription may not be present - request reply uses
            // a sync subscription which won't be added to the wrappers.
            if (wrappers.ContainsKey(s))
            {
                wrappers.Remove(s);
            }

            base.removeSub(s);
        }

        /// <summary>
        /// Gets or sets the method which is called to serialize
        /// objects sent as a message payload.
        /// </summary>
        /// <remarks>If <c>null</c> is given then the
        /// default serialization method for the platform is used, if one exists.</remarks>
        public Serializer OnSerialize
        {
            get
            {
                return onSerialize;
            }
            set
            {
                if (value == null)
                    onSerialize = defaultSerializer;
                else
                    onSerialize = value;
            }
        }

        /// <summary>
        /// Gets or sets the method which is called to deserialize
        /// objects from a message payload.
        /// </summary>
        /// <remarks>If <c>null</c> is given then the
        /// default deserialization method for the platform is used, if one exists.</remarks>
        public Deserializer OnDeserialize
        {
            get
            {
                return onDeserialize;
            }
            set
            {
                if (value == null)
                    onDeserialize = defaultDeserializer;
                else
                    onDeserialize = value;
            }
        }

        /// <summary>
        /// Closes the <see cref="EncodedConnection"/> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed
        /// and unmanaged resources; <c>false</c> to release only unmanaged 
        /// resources.</param>
        protected override void Dispose(bool disposing)
        {
            dStream.Dispose();
            sStream.Dispose();
            base.Dispose(disposing);
        }
    }
}