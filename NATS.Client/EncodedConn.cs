
using System;
using System.Collections.Generic;
using System.IO;
#if NET45
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
#endif
// disable XML comment warnings
#pragma warning disable 1591

namespace NATS.Client
{
    /// <summary>
    /// Overrides default binary serialization.
    /// </summary>
    /// <param name="obj">The object to serialize</param>
    public delegate byte[] Serializer(Object obj);

    /// <summary>
    /// Overrides the default binary deserialization.
    /// </summary>
    /// <param name="data">Data from the mesasge payload</param>
    /// <returns>The deserialized object.</returns>
    public delegate Object Deserializer(byte[] data);

    /// <summary>
    /// Event arguments for the EncodedConnection Asynchronous Subscriber delegate.
    /// </summary>
    public class EncodedMessageEventArgs : EventArgs
    {
        internal string  subject = null;
        internal string  reply = null;
        internal object  obj = null;
        internal Msg     msg = null;

        internal EncodedMessageEventArgs() {}

        /// <summary>
        /// Gets the subject this object was received on.
        /// </summary>
        public string Subject
        {
            get { return subject; }
        }

        /// <summary>
        /// Gets the reply
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
        /// Gets the original message this object was deserialized from.
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
    /// This class subclasses the Connection class to support serialization.
    /// </summary>
    public class EncodedConnection : Connection, IEncodedConnection
    {
        private MemoryStream sStream   = new MemoryStream();
        byte[] sBytes = new byte[1024];

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
            if (obj == null)
                return null;

            sStream.Position = 0;
            f.Serialize(sStream, obj);

            long len = sStream.Position;
            if (sBytes.Length < len)
                sBytes = new byte[len];

            // Could be more efficient, but w/o passing a length all the way
            // through publish, we have to do this.   No such thing as slices
            // in .NET
            Array.Copy(sStream.GetBuffer(), sBytes, len);

            return sBytes;
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
            lock (mu)
            {
                if (onSerialize == null)
                    throw new NATSException("IEncodedConnection.OnSerialize must be set (.NET core only).");

                byte[] data = onSerialize(o);
                publish(subject, reply, data);
            }
        }

        public void Publish(string subject, Object o)
        {
            publishObject(subject, null, o);
        }

        public void Publish(string subject, string reply, object o)
        {
            publishObject(subject, reply, o);
        }

        // Wrapper the handler for keeping a local copy of the event arguments around.
        internal class EncodedHandlerWrapper
        {
            EventHandler<EncodedMessageEventArgs> mh;
            EncodedConnection c;

            EncodedMessageEventArgs ehev = new EncodedMessageEventArgs();

            internal EncodedHandlerWrapper(EncodedConnection encc, EventHandler<EncodedMessageEventArgs> handler)
            {
                mh = handler;
                c = encc;
            }

            public void msgHandlerToEncoderHandler(Object sender, MsgHandlerEventArgs args)
            {
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
                throw new ArgumentException("Handler cannot be null.");

            if (onDeserialize == null)
                throw new NATSException("IEncodedConnection.OnDeserialize must be set before subscribing.");

            EncodedHandlerWrapper echWrapper = new EncodedHandlerWrapper(this, handler);

            IAsyncSubscription s = base.subscribeAsync(subject, reply,
                echWrapper.msgHandlerToEncoderHandler);

            wrappers.Add(s, echWrapper);

            return s;
        }

        public IAsyncSubscription SubscribeAsync(string subject, EventHandler<EncodedMessageEventArgs> handler)
        {
            return subscribeAsync(subject, null, handler);
        }

        public IAsyncSubscription SubscribeAsync(string subject, string queue, EventHandler<EncodedMessageEventArgs> handler)
        {
            return subscribeAsync(subject, queue, handler);
        }

        // lower level method to serialize an object, send a request,
        // and deserialize the returning object.
        private object requestObject(string subject, object obj, int timeout)
        {
            byte[] data = onSerialize(obj);
            Msg m = base.request(subject, data, timeout);
            return onDeserialize(m.Data);
        }

        public object Request(string subject, object obj, int timeout)
        {
            return requestObject(subject, obj, timeout);
        }

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

        protected override void Dispose(bool disposing)
        {
            dStream.Dispose();
            sStream.Dispose();
            base.Dispose(disposing);
        }
    }
}