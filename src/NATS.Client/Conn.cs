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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATS.Client
{
    /// <summary>
    /// State of the <see cref="IConnection"/>.
    /// </summary>
    public enum ConnState
    {
        /// <summary>
        /// The <see cref="IConnection"/> is disconnected.
        /// </summary>
        DISCONNECTED = 0,
        
        /// <summary>
        /// The <see cref="IConnection"/> is connected to a NATS Server.
        /// </summary>
        CONNECTED,

        /// <summary>
        /// The <see cref="IConnection"/> has been closed.
        /// </summary>
        CLOSED,

        /// <summary>
        /// The <see cref="IConnection"/> is currently reconnecting
        /// to a NATS Server.
        /// </summary>
        RECONNECTING,

        /// <summary>
        /// The <see cref="IConnection"/> is currently connecting
        /// to a NATS Server.
        /// </summary>
        CONNECTING,

        /// <summary>
        /// The <see cref="IConnection"/> is currently draining subscriptions.
        /// </summary>
        DRAINING_SUBS,

        /// <summary>
        /// The <see cref="IConnection"/> is currently connecting draining
        /// publishers.
        /// </summary>
        DRAINING_PUBS
    }

    internal enum ClientProtcolVersion
    {
        // clientProtoZero is the original client protocol from 2009.
 	    // http://nats.io/documentation/internals/nats-protocol/
        ClientProtoZero = 0,

        // ClientProtoInfo signals a client can receive more then the original INFO block.
        // This can be used to update clients on other cluster members, etc.
        ClientProtoInfo
    }

    /// <summary>
    /// <see cref="Connection"/> represents a bare connection to a NATS server.
    /// Users should create an <see cref="IConnection"/> instance using
    /// <see cref="ConnectionFactory"/> rather than directly using this class.
    /// </summary>
    // TODO - for a pure object model, we can create
    // an abstract subclass containing shared code between conn and 
    // encoded conn rather than using this class as
    // a base class.  This can happen anytime as we are using
    // interfaces.
    public class Connection : IConnection, IDisposable
    {
        Statistics stats = new Statistics();

        // NOTE: We aren't using Mutex here to support enterprises using
        // .NET 4.0.
        private readonly object mu = new Object();

        private readonly Nuid _nuid = new Nuid();

        Options opts = new Options();

        /// <summary>
        /// Gets the configuration options for this instance.
        /// </summary>
        public Options Opts
        {
            get { return opts; }
        }

        private readonly List<Thread> wg = new List<Thread>(2);

        private Uri             url     = null;
        private ServerPool srvPool = new ServerPool();

        // we have a buffered reader for writing, and reading.
        // This is for both performance, and having to work around
        // interlinked read/writes (supported by the underlying network
        // stream, but not the BufferedStream).
        private Stream  bw      = null;
        private Stream  br      = null;
        private MemoryStream    pending = null;

        Object flusherLock     = new Object();
        bool   flusherKicked = false;
        bool   flusherDone     = false;

        private ServerInfo     info = null;
        private Int64          ssid = 0;

        private Dictionary<Int64, Subscription> subs = 
            new Dictionary<Int64, Subscription>();
        
        private readonly ConcurrentQueue<SingleUseChannel<bool>> pongs = new ConcurrentQueue<SingleUseChannel<bool>>();

        internal MsgArg   msgArgs = new MsgArg();

        internal ConnState status = ConnState.CLOSED;

        internal Exception lastEx;

        Timer               ptmr = null;
        int                 pout = 0;

        internal static Random random = new Random();

        private AsyncSubscription globalRequestSubscription;
        private readonly string globalRequestInbox;

        // used to map replies to requests from client (should lock)
        private long nextRequestId = 0;

        private readonly Dictionary<string, InFlightRequest> waitingRequests
            = new Dictionary<string, InFlightRequest>(StringComparer.OrdinalIgnoreCase);

        // Prepare protocol messages for efficiency
        private byte[] PING_P_BYTES = null;
        private int    PING_P_BYTES_LEN;

        private byte[] PONG_P_BYTES = null;
        private int    PONG_P_BYTES_LEN;

        private byte[] PUB_P_BYTES = null;
        private int    PUB_P_BYTES_LEN = 0;

        private byte[] HPUB_P_BYTES = null;
        private int    HPUB_P_BYTES_LEN = 0;

        private byte[] CRLF_BYTES = null;
        private int    CRLF_BYTES_LEN = 0;

        byte[] pubProtoBuf = null;

        static readonly int REQ_CANCEL_IVL = 100;

        // 60 second default flush timeout
        static readonly int DEFAULT_FLUSH_TIMEOUT = 10000;

        TCPConnection conn = new TCPConnection();

        SubChannelPool subChannelPool = null;

        // One could use a task scheduler, but this is simpler and will
        // likely be easier to port to .NET core.
        private class CallbackScheduler : IDisposable
        {
            private readonly object runningLock = new object();
            private readonly Channel<Action> tasks = new Channel<Action>() { Name = "Tasks" };

            private Task executorTask     = null;
            private bool schedulerRunning = false;

            private bool Running
            {
                get
                {
                    lock (runningLock)
                    {
                        return schedulerRunning;
                    }
                }

                set
                {
                    lock  (runningLock)
                    {
                        schedulerRunning = value;
                    }
                }
            }

            private void process()
            {
                while (Running)
                {
                    Action action = tasks.get(-1);
                    try
                    {
                        action();
                    }
                    catch (Exception) { }
                }
            }

            internal void Start()
            {
                lock (runningLock)
                {
                    schedulerRunning = true;

                    // Use the default task scheduler and do not let child tasks launched
                    // when running actions to attach to this task (Issue #273)
                    executorTask = Task.Factory.StartNew(
                        process, 
                        CancellationToken.None,
                        TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                        TaskScheduler.Default);
                }
            }

            internal void Add(Action action)
            {
                lock (runningLock)
                {
                    if (schedulerRunning)
                        tasks.add(action);
                }
            }

            internal void ScheduleStop()
            {
                Add(StopInternal);
            }

            private void StopInternal()
            {
                Running = false;
                tasks.close();
            }

            internal void WaitForCompletion()
            {
                try
                {
                    executorTask.Wait(5000);
                }
                catch (Exception) { }
            }

            #region IDisposable Support
            private bool disposedValue = false; // To detect redundant calls

            protected virtual void Dispose(bool disposing)
            {
                if (!disposedValue)
                {
#if NET46
                    if (executorTask != null)
                        executorTask.Dispose();
#endif

                    disposedValue = true;
                }
            }

            public void Dispose()
            {
                Dispose(true);
            }
            #endregion
        }

        CallbackScheduler callbackScheduler = new CallbackScheduler();

        internal class Control
        {
            // for efficiency, assign these once in the contructor;
            internal string op;
            internal string args;

            static readonly internal char[] separator = { ' ' };

            // ensure this object is always created with a string.
            private Control() { }

            internal Control(string s)
            {
                string[] parts = s.Split(separator, 2);

                if (parts.Length == 1)
                {
                    op = parts[0].Trim();
                    args = IC._EMPTY_;
                }
                if (parts.Length == 2)
                {
                    op = parts[0].Trim();
                    args = parts[1].Trim();
                }
                else
                {
                    op = IC._EMPTY_;
                    args = IC._EMPTY_;
                }
            }
        }

        /// <summary>
        /// Convenience class representing the TCP connection to prevent 
        /// managing two variables throughout the NATs client code.
        /// </summary>
        private sealed class TCPConnection : IDisposable
        {
            /// A note on the use of streams.  .NET provides a BufferedStream
            /// that can sit on top of an IO stream, in this case the network
            /// stream. It increases performance by providing an additional
            /// buffer.
            /// 
            /// So, here's what we have for writing:
            ///     Client code
            ///          ->BufferedStream (bw)
            ///              ->NetworkStream/SslStream (srvStream)
            ///                  ->TCPClient (srvClient);
            ///                  
            ///  For reading:
            ///     Client code
            ///          ->NetworkStream/SslStream (srvStream)
            ///              ->TCPClient (srvClient);
            /// 
            object        mu        = new object();
            TcpClient     client    = null;
            NetworkStream stream    = null;
            SslStream     sslStream = null;

            string        hostName  = null;

            internal void open(Srv s, int timeoutMillis)
            {
                lock (mu)
                {
                    // If a connection was lost during a reconnect we 
                    // we could have a defunct SSL stream remaining and 
                    // need to clean up.
                    if (sslStream != null)
                    {
                        try
                        {
                            sslStream.Dispose();
                        }
                        catch (Exception) { }
                        sslStream = null;
                    }

                    client = new TcpClient(Socket.OSSupportsIPv6 ? AddressFamily.InterNetworkV6 : AddressFamily.InterNetwork);
                    if (Socket.OSSupportsIPv6)
                        client.Client.DualMode = true;

                    var task = client.ConnectAsync(s.url.Host, s.url.Port);
                    // avoid raising TaskScheduler.UnobservedTaskException if the timeout occurs first
                    task.ContinueWith(t => GC.KeepAlive(t.Exception), TaskContinuationOptions.OnlyOnFaulted);
                    if (!task.Wait(TimeSpan.FromMilliseconds(timeoutMillis)))
                    {
                        close(client);
                        client = null;
                        throw new NATSConnectionException("timeout");
                    }

                    client.NoDelay = false;

                    client.ReceiveBufferSize = Defaults.defaultBufSize*2;
                    client.SendBufferSize    = Defaults.defaultBufSize;
                    
                    stream = client.GetStream();

                    // save off the hostname
                    hostName = s.url.Host;
                }
            }

            private static bool remoteCertificateValidation(
                  object sender,
                  X509Certificate certificate,
                  X509Chain chain,
                  SslPolicyErrors sslPolicyErrors)
            {
                if (sslPolicyErrors == SslPolicyErrors.None)
                    return true;

                return false;
            }

            internal static void close(TcpClient c)
            {
#if NET46
                    c?.Close();
#else
                    c?.Dispose();
#endif
                c = null;
            }

            internal void makeTLS(Options options)
            {
                RemoteCertificateValidationCallback cb = null;

                if (stream == null)
                    throw new NATSException("Internal error:  Cannot create SslStream from null stream.");

                cb = options.TLSRemoteCertificationValidationCallback;
                if (cb == null)
                    cb = remoteCertificateValidation;

                sslStream = new SslStream(stream, false, cb, null,
                    EncryptionPolicy.RequireEncryption);

                try
                {
                    SslProtocols protocol = (SslProtocols)Enum.Parse(typeof(SslProtocols), "Tls12");
                    sslStream.AuthenticateAsClientAsync(hostName, options.certificates, protocol, true).Wait();
                }
                catch (Exception ex)
                {
                    sslStream.Dispose();
                    sslStream = null;

                    close(client);
                    client = null;
                    throw new NATSConnectionException("TLS Authentication error", ex);
                }
            }

            internal int SendTimeout
            {
                set
                {
                    if (client != null)
                        client.SendTimeout = value;
                }
            }

            internal int ReceiveTimeout
            {
                get
                {
                    if(client == null)
                        throw new InvalidOperationException("Connection not properly initialized.");

                    return client.ReceiveTimeout;
                }
                set
                {
                    if (client != null)
                        client.ReceiveTimeout = value;
                }
            }

            internal bool isSetup()
            {
                return (client != null);
            }

            internal void teardown()
            {
                TcpClient c;
                Stream s;

                lock (mu)
                {
                    c = client;
                    s = getReadBufferedStream();

                    client = null;
                    stream = null;
                    sslStream = null;
                }

                try
                {
                    if (s != null)
                        s.Dispose();

                    if (c != null)
                        close(c);
                }
                catch (Exception) { }
            }

            internal Stream getReadBufferedStream()
            {
                if (sslStream != null)
                    return sslStream;

                return stream;
            }

            internal Stream getWriteBufferedStream(int size)
            {

                BufferedStream bs = null;
                if (sslStream != null)
                    bs = new BufferedStream(sslStream, size);
                else
                    bs = new BufferedStream(stream, size);

                return bs;
            }

            internal bool Connected
            {
                get
                {
                    var tmp = client;
                    if (tmp == null)
                        return false;

                    return tmp.Connected;
                }
            }

            internal bool DataAvailable
            {
                get
                {
                    var tmp = stream;
                    if (tmp == null)
                        return false;

                    return tmp.DataAvailable;
                }
            }

            #region IDisposable Support
            private bool disposedValue = false; // To detect redundant calls

            void Dispose(bool disposing)
            {
                if (!disposedValue)
                {
                    if (sslStream != null)
                        sslStream.Dispose();
                    if (stream != null)
                        stream.Dispose();
                    if (client != null)
                    {
                        close(client);
                        client = null;
                    }

                    disposedValue = true;
                }
            }

            public void Dispose()
            {
                Dispose(true);
            }
            #endregion
        }

        /// <summary>
        /// The SubChannelPool class is used when the application
        /// has specified async subscribers will share channels and associated
        /// processing threads in the connection.  It simply returns a channel 
        /// that already has a long running task (thread) processing it.  
        /// Async subscribers use this channel in lieu of their own channel and
        /// message processing task.
        /// </summary>
        internal sealed class SubChannelPool : IDisposable
        {
            /// <summary>
            /// SubChannelProcessor creates a channel and a task to process
            /// messages on that channel.
            /// </summary>
            // TODO:  Investigate reuse of this class in async sub.
            private sealed class SubChannelProcessor : IDisposable
            {
                private readonly Connection connection;
                private Task channelTask = null;

                internal SubChannelProcessor(Connection c)
                {
                    connection = c;

                    Channel = new Channel<Msg>()
                    {
                        Name = "SubChannelProcessor " + this.GetHashCode(),
                    };

                    // Use the default task scheduler and do not let child tasks launched
                    // when delivering messages to attach to this task (Issue #273)
                    channelTask = Task.Factory.StartNew(
                        DeliverMessages, 
                        CancellationToken.None,
                        TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                        TaskScheduler.Default);
                }

                internal Channel<Msg> Channel { get; }

                private void DeliverMessages() => connection.deliverMsgs(Channel);

                public void Dispose()
                {
                    // closing the channel will end the task, but cap the
                    // wait just in case things are slow.  
                    // See Connection.deliverMsgs
                    Channel.close();
                    channelTask.Wait(500);
#if NET46
                    channelTask.Dispose();
#endif
                    channelTask = null;
                }
            }

            object pLock = new object();
            List<SubChannelProcessor> pList = new List<SubChannelProcessor>();

            int current = 0;
            int maxTasks = 0;

            Connection connection = null;

            internal SubChannelPool(Connection c, int numTasks)
            {
                maxTasks = numTasks;
                connection = c;
            }

            /// <summary>
            /// Gets a message channel for use with an async subscriber.
            /// </summary>
            /// <returns>
            /// A channel, already setup with a task processing messages.
            /// </returns>
            internal Channel<Msg> getChannel()
            {
                // simple round robin, adding Channels/Tasks as necessary.
                lock (pLock)
                {
                    if (current == maxTasks)
                        current = 0;

                    if (pList.Count == current)
                        pList.Add(new SubChannelProcessor(connection));

                    return pList[current++].Channel;
                }
            }

            public void Dispose()
            {
                lock (pLock)
                {
                    pList.ForEach((p) => { p.Dispose(); });
                    pList.Clear();
                }
            }
        }

        /// <summary>
        /// Gets an available message channel for use with async subscribers.  It will
        /// setup the message channel pool if configured to do so.
        /// </summary>
        /// <returns>
        /// A channel for use, null if configuration dictates not to use the 
        /// channel pool.
        /// </returns>
        internal Channel<Msg> getMessageChannel()
        {
            lock (mu)
            {
                if (opts.subscriberDeliveryTaskCount > 0 && subChannelPool == null)
                {
                    subChannelPool = new SubChannelPool(this,
                        opts.subscriberDeliveryTaskCount);
                }

                if (subChannelPool != null)
                {
                    return subChannelPool.getChannel();
                }
            }

            return null;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Connection"/> class
        /// with the specified <see cref="Options"/>.
        /// </summary>
        /// <param name="options">The configuration options to use for this 
        /// <see cref="Connection"/>.</param>
        internal Connection(Options options)
        {
            opts = new Options(options);
            if (opts.ReconnectDelayHandler == null)
            {
                opts.ReconnectDelayHandler = DefaultReconnectDelayHandler;
            }

            PING_P_BYTES = Encoding.UTF8.GetBytes(IC.pingProto);
            PING_P_BYTES_LEN = PING_P_BYTES.Length;

            PONG_P_BYTES = Encoding.UTF8.GetBytes(IC.pongProto);
            PONG_P_BYTES_LEN = PONG_P_BYTES.Length;

            PUB_P_BYTES = Encoding.UTF8.GetBytes(IC._PUB_P_);
            PUB_P_BYTES_LEN = PUB_P_BYTES.Length;

            HPUB_P_BYTES = Encoding.UTF8.GetBytes(IC._HPUB_P_);
            HPUB_P_BYTES_LEN = HPUB_P_BYTES.Length;

            CRLF_BYTES = Encoding.UTF8.GetBytes(IC._CRLF_);
            CRLF_BYTES_LEN = CRLF_BYTES.Length;

            // predefine the start of the publish protocol message.
            buildPublishProtocolBuffers(512);

            callbackScheduler.Start();

            globalRequestInbox = NewInbox();
        }

        private void buildPublishProtocolBuffers(int size)
        {
            pubProtoBuf = new byte[size];
            Buffer.BlockCopy(HPUB_P_BYTES, 0, pubProtoBuf, 0, HPUB_P_BYTES_LEN);
        }

        // Ensures that pubProtoBuf is appropriately sized for the given
        // subject and reply.
        // Caller must lock.
        private void ensurePublishProtocolBuffer(string subject, string reply)
        {
            // Publish protocol buffer sizing:
            //
            // HPUB_P_BYTES_LEN (includes trailing space)
            //  + SUBJECT field length
            //  + SIZE field maximum + 1 (= log(2147483647) + 1 = 11)
            //  + (optional) REPLY field length + 1

            int pubProtoBufSize = HPUB_P_BYTES_LEN
                                + (1 + subject.Length)
                                + (34) // Include payload/header sizes and stamp
                                + (reply != null ? reply.Length + 1 : 0);

            // only resize if we're increasing the buffer...
            if (pubProtoBufSize > pubProtoBuf.Length)
            {
                // ...and when we increase it up to the next power of 2.
                pubProtoBufSize--;
                pubProtoBufSize |= pubProtoBufSize >> 1;
                pubProtoBufSize |= pubProtoBufSize >> 2;
                pubProtoBufSize |= pubProtoBufSize >> 4;
                pubProtoBufSize |= pubProtoBufSize >> 8;
                pubProtoBufSize |= pubProtoBufSize >> 16;
                pubProtoBufSize++;

                buildPublishProtocolBuffers(pubProtoBufSize);
            }
        }

        // Will assign the correct server to the Conn.Url
        private void pickServer()
        {
            url = null;

            Srv s = srvPool.First();
            if (s == null)
                throw new NATSNoServersException("Unable to choose server; no servers available.");

            url = s.url;
        }


        // Create the server pool using the options given.
        // We will place a Url option first, followed by any
        // Server Options. We will randomize the server pool unlesss
        // the NoRandomize flag is set.
        private void setupServerPool()
        {
            srvPool.Setup(Opts);
            pickServer();
        }

        // createConn will connect to the server and wrap the appropriate
        // bufio structures. It will do the right thing when an existing
        // connection is in place.
        private bool createConn(Srv s, out Exception ex)
        {
            ex = null;
            try
            {
                conn.open(s, opts.Timeout);

                if (pending != null && bw != null)
                {
                    // flush to the pending buffer;
                    try
                    {
                        // Make a best effort, but this shouldn't stop
                        // conn creation.
                        bw.Flush();
                    }
                    catch (Exception) { }
                }

                bw = conn.getWriteBufferedStream(Defaults.defaultBufSize);
                br = conn.getReadBufferedStream();
            }
            catch (Exception e)
            {
                ex = e;
                return false;
            }

            return true;
        }

        // makeSecureConn will wrap an existing Conn using TLS
        private void makeTLSConn()
        {
            conn.makeTLS(this.opts);

            bw = conn.getWriteBufferedStream(Defaults.defaultBufSize);
            br = conn.getReadBufferedStream();
        }

        // waitForExits will wait for all socket watcher Go routines to
        // be shutdown before proceeding.
        private void waitForExits()
        {
            // Kick old flusher forcefully.
            setFlusherDone(true);

            if (wg.Count <= 0)
                return;

            var cpy = wg.ToArray();

            foreach (var t in cpy)
            {
                try
                {
                    t.Join();
                    wg.Remove(t);
                }
                catch (Exception)
                {
                    // ignored
                }
            }
        }

        private void pingTimerCallback(object state)
        {
            lock (mu)
            {
                if (status != ConnState.CONNECTED)
                {
                    return;
                }

                if (Interlocked.Increment(ref pout) > Opts.MaxPingsOut)
                {
                    processOpError(new NATSStaleConnectionException());
                    return;
                }

                try
                {
                    sendPing(null);
                }
                catch (Exception) { }
                //trigger timer again
                if (ptmr != null)
                {
                    ptmr.Change(Opts.PingInterval, Timeout.Infinite);
                }
            }
        }

        // caller must lock
        private void stopPingTimer()
        {
            if (ptmr != null)
            {
                ptmr.Dispose();
                ptmr = null;
            }
        }

        // caller must lock
        private void startPingTimer()
        {
            Interlocked.Exchange(ref pout, 0);

            stopPingTimer();

            if (Opts.PingInterval > 0)
            {
                ptmr = new Timer(pingTimerCallback, null,
                    opts.PingInterval,
                    Timeout.Infinite); // do not trigger timer automatically : risk of having too many threads spawn during reconnect
            }
        }

        private string generateThreadName(string prefix)
        {
            string name = "unknown";
            if (opts.Name != null)
            {
                name = opts.Name;
            }

            return prefix + "-" + name;
        }

        private void spinUpSocketWatchers()
        {
            Thread t = null;

            waitForExits();

            // Ensure threads are started before we continue with
            // ManualResetEvents.
            ManualResetEvent readLoopStartEvent = new ManualResetEvent(false);
            t = new Thread(() => {
                readLoopStartEvent.Set();
                readLoop();
            });
            t.IsBackground = true;
            t.Start();
            t.Name = generateThreadName("Reader");
            wg.Add(t);

            ManualResetEvent flusherStartEvent = new ManualResetEvent(false);
            t = new Thread(() => {
                flusherStartEvent.Set();
                flusher();
            });
            t.IsBackground = true;
            t.Start();
            t.Name = generateThreadName("Flusher");
            wg.Add(t);

            // wait for both threads to start before continuing.
            flusherStartEvent.WaitOne(60000);
            readLoopStartEvent.WaitOne(60000);
        }

        /// <summary>
        /// Gets the URL of the NATS server to which this instance
        /// is connected, otherwise <c>null</c>.
        /// </summary>
        public string ConnectedUrl
        {
            get
            {
                lock (mu)
                {
                    if (status != ConnState.CONNECTED)
                        return null;

                    return url.OriginalString;
                }
            }
        }

        /// <summary>
        /// Gets the IP of client as known by the NATS server, otherwise <c>null</c>.
        /// </summary>
        /// <remarks>
        /// Supported in the NATS server version 2.1.6 and above.  If the client is connected to
        /// an older server or is in the process of connecting, null will be returned.
        /// </remarks>
        public IPAddress ClientIP
        {
            get
            {
                string clientIp;
                lock (mu)
                {
                    clientIp = info.ClientIp;
                }
                
                return !String.IsNullOrEmpty(clientIp) ? IPAddress.Parse(clientIp) : null;
            }
        }

        /// <summary>
        /// Gets the server ID of the NATS server to which this instance
        /// is connected, otherwise <c>null</c>.
        /// </summary>
        public string ConnectedId
        {
            get
            {
                lock (mu)
                {
                    if (status != ConnState.CONNECTED)
                        return IC._EMPTY_;

                    return this.info.ServerId;
                }
            }
        }

        /// <summary>
        /// Gets an array of known server URLs for this instance.
        /// </summary>
        /// <remarks><see cref="Servers"/> also includes any additional
        /// servers discovered after a connection has been established. If
        /// authentication is enabled, <see cref="Options.User"/> or
        /// <see cref="Options.Token"/> must be used when connecting with
        /// these URLs.</remarks>
        public string[] Servers
        {
            get
            {
                lock (mu)
                {
                    return srvPool.GetServerList(false);
                }
            }
        }

        /// <summary>
        /// Gets an array of server URLs that were discovered after this
        /// instance connected.
        /// </summary>
        /// <remarks>If authentication is enabled, <see cref="Options.User"/> or
        /// <see cref="Options.Token"/> must be used when connecting with
        /// these URLs.</remarks>
        public string[] DiscoveredServers
        {
            get
            {
                lock (mu)
                {
                    return srvPool.GetServerList(true);
                }
            }
        }

        // Process a connected connection and initialize properly.
        // Caller must lock.
        private void processConnectInit(Srv s)
        {
            this.status = ConnState.CONNECTING;

            var orgTimeout = conn.ReceiveTimeout;

            try
            {
                conn.ReceiveTimeout = opts.Timeout;
                processExpectedInfo(s);
                sendConnect();
            }
            catch (IOException ex)
            {
                throw new NATSConnectionException("Error while performing handshake with server. See inner exception for more details.", ex);
            }
            finally
            {
                if(conn.isSetup())
                    conn.ReceiveTimeout = orgTimeout;
            }

            // .NET vs go design difference here:
            // Starting the ping timer earlier allows us
            // to assign, and thus, dispose of it if the connection 
            // is disposed before the socket watchers are running.
            // Otherwise, the connection is referenced by an orphaned 
            // ping timer which can create a memory leak.
            startPingTimer();

            Task.Factory.StartNew(
                spinUpSocketWatchers,
                CancellationToken.None,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default);
        }

        internal bool connect(Srv s, out Exception exToThrow)
        {
            url = s.url;
            try
            {
                exToThrow = null;

                NATSConnectionException natsAuthEx = null;

                for(var i = 0; i < 6; i++) //Precaution to not end up in server returning ExTypeA, ExTypeB, ExTypeA etc.
                {
                    try
                    {
                        lock (mu)
                        {
                            if (!createConn(s, out exToThrow))
                                return false;

                            processConnectInit(s);
                            exToThrow = null;

                            return true;
                        }
                    }
                    catch (NATSConnectionException ex)
                    {
                        if (!ex.IsAuthorizationViolationError() && !ex.IsAuthenticationExpiredError())
                            throw;

                        var aseh = opts.AsyncErrorEventHandler;
                        if (aseh != null)
                            callbackScheduler.Add(() => aseh(s, new ErrEventArgs(this, null, ex.Message)));

                        if (natsAuthEx == null || !natsAuthEx.Message.Equals(ex.Message, StringComparison.OrdinalIgnoreCase))
                        {
                            natsAuthEx = ex;
                            continue;
                        }

                        throw;
                    }
                }
            }
            catch (Exception ex)
            {
                exToThrow = ex;
                close(ConnState.DISCONNECTED, false, ex);
                lock (mu)
                {
                    url = null;
                }
            }

            return false;
        }

        internal void connect()
        {
            Exception exToThrow = null;

            setupServerPool();

            srvPool.ConnectToAServer((s) => {
                return connect(s, out exToThrow);
            });

            lock (mu)
            {
                if (status != ConnState.CONNECTED)
                {
                    if (exToThrow is NATSException)
                        throw exToThrow;
                    if (exToThrow != null)
                        throw new NATSConnectionException("Failed to connect", exToThrow);
                    throw new NATSNoServersException("Unable to connect to a server.");
                }
            }
        }

        // This will check to see if the connection should be
        // secure. This can be dictated from either end and should
        // only be called after the INIT protocol has been received.
        private void checkForSecure(Srv s)
        {
            // Check to see if we need to engage TLS
            // Check for mismatch in setups
            if (Opts.Secure && !info.TlsRequired)
            {
                throw new NATSSecureConnWantedException();
            }
            else if (info.TlsRequired && !Opts.Secure)
            {
                // If the server asks us to be secure, give it
                // a shot.
                Opts.Secure = true;
            }

            // Need to rewrap with bufio if options tell us we need
            // a secure connection or the tls url scheme was specified.
            if (Opts.Secure || s.Secure)
            {
                makeTLSConn();
            }
        }

        // processExpectedInfo will look for the expected first INFO message
        // sent when a connection is established. The lock should be held entering.
        // Caller must lock.
        private void processExpectedInfo(Srv s)
        {
            Control c;

            try
            {
                conn.SendTimeout = 2;
                c = readOp();
            }
            catch (Exception e)
            {
                processOpError(e);
                throw;
            }
            finally
            {
                conn.SendTimeout = 0;
            }

            if (!IC._INFO_OP_.Equals(c.op))
            {
                throw new NATSConnectionException("Protocol exception, INFO not received");
            }

            // do not notify listeners of server changes when we process the first INFO message
            processInfo(c.args, false);
            checkForSecure(s);
        }

        private void writeString(string format, string a, string b)
        {
            writeString(string.Format(format, a, b));
        }

        private void writeString(string format, string a, string b, string c)
        {
            writeString(string.Format(format, a, b, c));
        }

        private void writeString(string value)
        {
            byte[] sendBytes = System.Text.Encoding.UTF8.GetBytes(value);
            bw.Write(sendBytes, 0, sendBytes.Length);
        }

        private void sendProto(byte[] value, int length)
        {
            lock (mu)
            {
                bw.Write(value, 0, length);
            }
        }

        // Generate a connect protocol message, issuing user/password if
        // applicable. The lock is assumed to be held upon entering.
        private string connectProto()
        {
            string u = url.UserInfo;
            string user = null;
            string pass = null;
            string token = null;
            string nkey = null;
            string sig = null;

            if (!string.IsNullOrEmpty(u))
            {
                if (u.Contains(":"))
                {
                    string[] userpass = u.Split(':');
                    if (userpass.Length > 0)
                    {
                        user = userpass[0];
                    }
                    if (userpass.Length > 1)
                    {
                        pass = userpass[1];
                    }
                }
                else
                {
                    token = u;
                }
            }
            else
            {
                user = opts.user;
                pass = opts.password;
                token = opts.token;
                nkey = opts.nkey;
            }

            // Look for user jwt
            string userJWT = null;
            if (opts.UserJWTEventHandler != null)
            {
                if (nkey != null)
                    throw new NATSConnectionException("User event handler and Nkey has been defined.");

                var args = new UserJWTEventArgs();
                try
                {
                    opts.UserJWTEventHandler(this, args);
                }
                catch (Exception ex)
                {
                    throw new NATSConnectionException("Exception from UserJWTEvent Handler.", ex);
                }
                userJWT = args.JWT;
                if (userJWT == null)
                    throw new NATSConnectionException("User JWT Event Handler did not set the JWT.");

            }

            if (userJWT != null || nkey != null)
            {
                if (opts.UserSignatureEventHandler == null)
                {
                    if (userJWT == null) {
                        throw new NATSConnectionException("Nkey defined without a user signature event handler");
                    }
                    throw new NATSConnectionException("User signature event handle has not been been defined.");
                }

                var args = new UserSignatureEventArgs(Encoding.ASCII.GetBytes(this.info.Nonce));
                try
                {
                    opts.UserSignatureEventHandler(this, args);
                }
                catch (Exception ex)
                {
                    throw new NATSConnectionException("Exception from UserSignatureEvent Handler.", ex);
                }

                if (args.SignedNonce == null)
                    throw new NATSConnectionException("Signature Event Handler did not set the SignedNonce.");

                sig = NaCl.CryptoBytes.ToBase64String(args.SignedNonce);               
            }

            ConnectInfo info = new ConnectInfo(opts.Verbose, opts.Pedantic, userJWT, nkey, sig, user,
                pass, token, opts.Secure, opts.Name, !opts.NoEcho);

            if (opts.NoEcho && info.protocol < 1)
                throw new NATSProtocolException("Echo is not supported by the server.");

            var sb = new StringBuilder().Append(IC.conProtoNoCRLF).Append(" ");

            return info.AppendAsJsonTo(sb).Append(IC._CRLF_).ToString();
        }


        // caller must lock.
        private void sendConnect()
        {
            string protocolMsg = connectProto();
            try
            {
                writeString(protocolMsg);
                bw.Write(PING_P_BYTES, 0, PING_P_BYTES_LEN);
                bw.Flush();
            }
            catch (Exception ex)
            {
                if (lastEx == null)
                    throw new NATSException("Error sending connect protocol message", ex);
            }

            string result = null;
            StreamReader sr = null;
            try
            {
                // TODO:  Make this reader (or future equivalent) unbounded.
                // we need the underlying stream, so leave it open.
                sr = new StreamReader(br, Encoding.UTF8, false, Defaults.MaxControlLineSize, true);
                result = sr.ReadLine();

                // If opts.verbose is set, handle +OK.
                if (opts.Verbose == true && IC.okProtoNoCRLF.Equals(result))
                {
                    result = sr.ReadLine();
                }
            }
            catch (Exception ex)
            {
                throw new NATSConnectionException("Connect read error", ex);
            }
            finally
            {
                if (sr != null)
                    sr.Dispose();
            }

            if (IC.pongProtoNoCRLF.Equals(result))
            {
                status = ConnState.CONNECTED;
                return;
            }
            else
            {
                if (result == null)
                {
                    throw new NATSConnectionException("Connect read protocol error");
                }
                else if (result.StartsWith(IC._ERR_OP_))
                {
                    throw new NATSConnectionException(
                        result.Substring(IC._ERR_OP_.Length).TrimStart(' '));
                }
                else
                {
                    throw new NATSException("Error from sendConnect(): " + result);
                }
            }
        }

        private Control readOp()
        {
            // This is only used when creating a connection, so simplify
            // life and just create a stream reader to read the incoming
            // info string.  If this becomes part of the fastpath, read
            // the string directly using the buffered reader.
            //
            // Keep the underlying stream open.
            using (StreamReader sr = new StreamReader(br, Encoding.ASCII, false, Defaults.MaxControlLineSize, true))
            {
                return new Control(sr.ReadLine());
            }
        }

        private void processDisconnect()
        {
            status = ConnState.DISCONNECTED;
            if (lastEx == null)
                return;
        }

        // This will process a disconnect when reconnect is allowed.
        // The lock should not be held on entering this function.
        private void processReconnect()
        {
            lock (mu)
            {
                // If we are already in the proper state, just return.
                if (isReconnecting())
                    return;

                status = ConnState.RECONNECTING;

                stopPingTimer();

                if (conn.isSetup())
                {
                    conn.teardown();
                }

                // clear any queued pongs, e..g. pending flush calls.
                clearPendingFlushCalls();

                pending = new MemoryStream();
                bw = new BufferedStream(pending);

                Thread t = new Thread(() =>
                {
                    doReconnect();
                });
                t.IsBackground = true;
                t.Name = generateThreadName("Reconnect");
                t.Start();
            }
        }

        // flushReconnectPending will push the pending items that were
        // gathered while we were in a RECONNECTING state to the socket.
        private void flushReconnectPendingItems()
        {
            if (pending == null)
                return;

            if (pending.Length > 0)
            {
#if NET46
                bw.Write(pending.GetBuffer(), 0, (int)pending.Length);
                bw.Flush();
#else
                ArraySegment<byte> buffer;
                if (pending.TryGetBuffer(out buffer))
                {
                    // TODO:  Buffer.length
                    bw.Write(buffer.Array, buffer.Offset, (int)buffer.Count);
                }
#endif
            }

            pending = null;
        }


        // Sleep guarantees no exceptions will be thrown out of it.
        private static void sleep(int millis)
        {
            try
            {
                Thread.Sleep(millis);
            }
            catch (Exception) { }
        }

        // Schedules a connection event (connected/disconnected/reconnected)
        // if non-null.
        // Caller must lock.
        private void scheduleConnEvent(EventHandler<ConnEventArgs> connEvent, Exception error = null)
        {
            // Schedule a reference to the event handler.
            EventHandler<ConnEventArgs> eh = connEvent;
            if (eh != null)
            {
                callbackScheduler.Add(
                    () => { eh(this, new ConnEventArgs(this, error)); }
                );
            }
        }

        private void DefaultReconnectDelayHandler(object o, ReconnectDelayEventArgs args)
        {
            int jitter = srvPool.HasSecureServer() ? random.Next(opts.ReconnectJitterTLS) : random.Next(opts.ReconnectJitter);

            Thread.Sleep(opts.ReconnectWait + jitter);
        }

        // Try to reconnect using the option parameters.
        // This function assumes we are allowed to reconnect.
        //
        // NOTE: locks are manually acquired/released to parallel the NATS
        // go client
        private void doReconnect()
        {
            // We want to make sure we have the other watchers shutdown properly
            // here before we proceed past this point
            waitForExits();

            // FIXME(dlc) - We have an issue here if we have
            // outstanding flush points (pongs) and they were not
            // sent out, but are still in the pipe.

            // Hold manually release where needed below.

            var lockWasTaken = false;

            try
            {
                Monitor.Enter(mu, ref lockWasTaken);

                //Keep ref to any error before clearing.
                var errorForHandler = lastEx;
                lastEx = null;

                scheduleConnEvent(Opts.DisconnectedEventHandler, errorForHandler);

                Srv cur;
                int wlf = 0;
                bool doSleep = false;
                while ((cur = srvPool.SelectNextServer(Opts.MaxReconnect)) != null)
                {
                    // check if we've been through the list
                    if (cur == srvPool.First())
                    {
                        doSleep = (wlf != 0);
                        wlf++;
                    }
                    else
                    {
                        doSleep = false;
                    }

                    url = cur.url;
                    lastEx = null;

                    if (lockWasTaken)
                    {
                        Monitor.Exit(mu);
                        lockWasTaken = false;
                    }

                    if (doSleep)
                    {
                        try
                        {
                            // If unset, the default handler will be called which uses an
                            // auto reset event to wait, unless kicked out of a close
                            // call.
                            opts?.ReconnectDelayHandler(this, new ReconnectDelayEventArgs(wlf - 1));
                        }
                        catch { } // swallow user exceptions
                    }

                    Monitor.Enter(mu, ref lockWasTaken);

                    if (isClosed())
                        break;

                    cur.reconnects++;

                    try
                    {
                        // try to create a new connection
                        if(!createConn(cur, out lastEx))
                            continue;
                    }
                    catch (Exception)
                    {
                        // not yet connected, retry and hold
                        // the lock.
                        lastEx = null;
                        continue;
                    }

                    // process our connect logic
                    try
                    {
                        processConnectInit(cur);
                    }
                    catch (Exception e)
                    {
                        lastEx = e;
                        status = ConnState.RECONNECTING;
                        continue;
                    }

                    try
                    {
                        // Send existing subscription state
                        resendSubscriptions();

                        // Now send off and clear pending buffer
                        flushReconnectPendingItems();
                    }
                    catch (Exception)
                    {
                        status = ConnState.RECONNECTING;
                        continue;
                    }

                    // We are reconnected.
                    stats.reconnects++;
                    cur.didConnect = true;
                    cur.reconnects = 0;
                    srvPool.CurrentServer = cur;
                    status = ConnState.CONNECTED;

                    scheduleConnEvent(Opts.ReconnectedEventHandler);

                    // Release lock here, we will return below
                    if (lockWasTaken)
                    {
                        Monitor.Exit(mu);
                        lockWasTaken = false;
                    }

                    // Make sure to flush everything
                    // We have a corner case where the server we just
                    // connected to has failed as well - let the reader
                    // thread detect this and spawn another reconnect 
                    // thread to simplify locking.
                    try
                    {
                        Flush();
                    }
                    catch
                    {
                        // ignored
                    }

                    return;
                }

                // Call into close.. we have no more servers left..
                if (lastEx == null)
                    lastEx = new NATSNoServersException("Unable to reconnect");

                Close();
            }
            finally
            {
                if(lockWasTaken)
                    Monitor.Exit(mu);
            }
        }

        private bool isConnecting()
        {
            return (status == ConnState.CONNECTING);
        }

        private void processOpError(Exception e)
        {
            bool disconnected = false;

            lock (mu)
            {
                if (isConnecting() || isClosed() || isReconnecting())
                {
                    return;
                }

                if (Opts.AllowReconnect && status == ConnState.CONNECTED)
                {
                    processReconnect();
                }
                else
                {
                    processDisconnect();
                    disconnected = true;
                    lastEx = e;
                }
            }

            if (disconnected)
            {
                Close();
            }
        }

        private void readLoop()
        {
            // Stack based buffer.
            byte[] buffer = new byte[Defaults.defaultReadLength];
            var parser = new Parser(this);
            int len;

            while (true)
            {
                try
                {
                    len = br.Read(buffer, 0, Defaults.defaultReadLength);

                    // A length of zero can mean that the socket was closed
                    // locally by the application (Close) or the server
                    // gracefully closed the socket.  There are some cases
                    // on windows where a server could take an exit path that
                    // gracefully closes sockets.  Throw an exception so we
                    // can reconnect.  If the network stream has been closed
                    // by the client, processOpError will do the right thing
                    // (nothing).
                    if (len == 0)
                    {
                        if (disposedValue || State == ConnState.CLOSED)
                            break;

                        throw new NATSConnectionException("Server closed the connection.");
                    }

                    parser.parse(buffer, len);
                }
                catch (Exception e)
                {
                    if (State != ConnState.CLOSED)
                    {
                        processOpError(e);
                    }

                    break;
                }
            }
        }

        // deliverMsgs waits on the delivery channel shared with readLoop and processMsg.
        // It is used to deliver messages to asynchronous subscribers.
        internal void deliverMsgs(Channel<Msg> ch)
        {
            int batchSize;
            lock (mu)
            {
                batchSize = opts.subscriptionBatchSize;
            }

            // dispatch buffer
            Msg[] mm = new Msg[batchSize];

            while (true)
            {
                if (isClosed())
                    return;

                int delivered = ch.get(-1, mm);
                if (delivered == 0)
                {
                    // the channel has been closed, exit silently.
                    return;
                }

                // Note, this seems odd message having the sub process itself, 
                // but this is good for performance.
                for (int ii = 0; ii < delivered; ++ii)
                {
                    Msg m = mm[ii];
                    mm[ii] = null; // do not hold onto the Msg any longer than we need to
                    if (!m.sub.processMsg(m))
                    {
                        lock (mu)
                        {
                            removeSub(m.sub);
                        }
                    }
                }
            }
        }

        // Roll our own fast conversion - we know it's the right
        // encoding. 
        char[] convertToStrBuf = new char[Defaults.MaxControlLineSize];

        // Since we know we don't need to decode the protocol string,
        // just copy the chars into bytes.  This increased
        // publish peformance by 30% as compared to Encoding.
        private int writeStringToBuffer(byte[] buffer, int offset, string value)
        {
            int length = value.Length;
            int end = offset + length;

            for (int i = 0; i < length; i++)
            {
                buffer[i+offset] = (byte)value[i];
            }

            return end;
        }

        #region Fast Unsigned Integer to UTF8 Conversion
        // Adapted from "C++ String Toolkit Library" using bit tricks
        // to avoid the string copy/reverse.
        /*
         *****************************************************************
         *                     String Toolkit Library                    *
         *                                                               *
         * Author: Arash Partow (2002-2017)                              *
         * URL: http://www.partow.net/programming/strtk/index.html       *
         *                                                               *
         * Copyright notice:                                             *
         * Free use of the String Toolkit Library is permitted under the *
         * guidelines and in accordance with the most current version of *
         * the MIT License.                                              *
         * http://www.opensource.org/licenses/MIT                        *
         *                                                               *
         *****************************************************************
        */

        static readonly byte[] rev_3digit_lut =
                           Encoding.UTF8.GetBytes(
                              "000001002003004005006007008009010011012013014015016017018019020021022023024"
                            + "025026027028029030031032033034035036037038039040041042043044045046047048049"
                            + "050051052053054055056057058059060061062063064065066067068069070071072073074"
                            + "075076077078079080081082083084085086087088089090091092093094095096097098099"
                            + "100101102103104105106107108109110111112113114115116117118119120121122123124"
                            + "125126127128129130131132133134135136137138139140141142143144145146147148149"
                            + "150151152153154155156157158159160161162163164165166167168169170171172173174"
                            + "175176177178179180181182183184185186187188189190191192193194195196197198199"
                            + "200201202203204205206207208209210211212213214215216217218219220221222223224"
                            + "225226227228229230231232233234235236237238239240241242243244245246247248249"
                            + "250251252253254255256257258259260261262263264265266267268269270271272273274"
                            + "275276277278279280281282283284285286287288289290291292293294295296297298299"
                            + "300301302303304305306307308309310311312313314315316317318319320321322323324"
                            + "325326327328329330331332333334335336337338339340341342343344345346347348349"
                            + "350351352353354355356357358359360361362363364365366367368369370371372373374"
                            + "375376377378379380381382383384385386387388389390391392393394395396397398399"
                            + "400401402403404405406407408409410411412413414415416417418419420421422423424"
                            + "425426427428429430431432433434435436437438439440441442443444445446447448449"
                            + "450451452453454455456457458459460461462463464465466467468469470471472473474"
                            + "475476477478479480481482483484485486487488489490491492493494495496497498499"
                            + "500501502503504505506507508509510511512513514515516517518519520521522523524"
                            + "525526527528529530531532533534535536537538539540541542543544545546547548549"
                            + "550551552553554555556557558559560561562563564565566567568569570571572573574"
                            + "575576577578579580581582583584585586587588589590591592593594595596597598599"
                            + "600601602603604605606607608609610611612613614615616617618619620621622623624"
                            + "625626627628629630631632633634635636637638639640641642643644645646647648649"
                            + "650651652653654655656657658659660661662663664665666667668669670671672673674"
                            + "675676677678679680681682683684685686687688689690691692693694695696697698699"
                            + "700701702703704705706707708709710711712713714715716717718719720721722723724"
                            + "725726727728729730731732733734735736737738739740741742743744745746747748749"
                            + "750751752753754755756757758759760761762763764765766767768769770771772773774"
                            + "775776777778779780781782783784785786787788789790791792793794795796797798799"
                            + "800801802803804805806807808809810811812813814815816817818819820821822823824"
                            + "825826827828829830831832833834835836837838839840841842843844845846847848849"
                            + "850851852853854855856857858859860861862863864865866867868869870871872873874"
                            + "875876877878879880881882883884885886887888889890891892893894895896897898899"
                            + "900901902903904905906907908909910911912913914915916917918919920921922923924"
                            + "925926927928929930931932933934935936937938939940941942943944945946947948949"
                            + "950951952953954955956957958959960961962963964965966967968969970971972973974"
                            + "975976977978979980981982983984985986987988989990991992993994995996997998999"
                            + "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
                         );

        static readonly byte[] rev_2digit_lut =
                                   Encoding.UTF8.GetBytes(
                                      "0001020304050607080910111213141516171819"
                                    + "2021222324252627282930313233343536373839"
                                    + "4041424344454647484950515253545556575859"
                                    + "6061626364656667686970717273747576777879"
                                    + "8081828384858687888990919293949596979899"
                                    + "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
                                );

        // Log(n) adapted from:
        // http://graphics.stanford.edu/~seander/bithacks.html#IntegerLog10
        static readonly int[] MultiplyDeBruijnBitPosition = new[]
            {
              0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
              8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
            };

        static readonly int[] PowersOf10 = new[]
            { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };

        // Fast conversion from an unsigned Int32 to a UTF8 string.
        // Assumes: non-negative.
        private int writeInt32ToBuffer(byte[] buffer, int offset, int value)
        {
            const int radix = 10;
            const int radix_sqr = radix * radix;
            const int radix_cube = radix * radix * radix;

            if (value >= 10)
            {
                // calculate Log10 to get the digit count (via Log2 and bit tricks)
                int v = value;
                v |= v >> 1;
                v |= v >> 2;
                v |= v >> 4;
                v |= v >> 8;
                v |= v >> 16;
                int lg2 = MultiplyDeBruijnBitPosition[(uint)(v * 0x07C4ACDD) >> 27];
                int t = (lg2 + 1) * 1233 >> 12;
                int lg10 = t - (value < PowersOf10[t] ? 1 : 0) + 1;

                // once we have the count of digits, start from the end of the output
                // number in the buffer
                int itr = offset + lg10;

                while (value >= radix_sqr)
                {
                    itr -= 3;
                    int temp_v = value / radix_cube;
                    int temp_off = 3 * (value - (temp_v * radix_cube));
                    buffer[itr]     = rev_3digit_lut[temp_off];
                    buffer[itr + 1] = rev_3digit_lut[temp_off + 1];
                    buffer[itr + 2] = rev_3digit_lut[temp_off + 2];
                    value = temp_v;
                }

                while (value >= radix)
                {
                    itr -= 2;
                    int temp_v = value / radix_sqr;
                    int temp_off = 2 * (value - (temp_v * radix_sqr));
                    buffer[itr]     = rev_2digit_lut[temp_off];
                    buffer[itr + 1] = rev_2digit_lut[temp_off + 1];
                    value = temp_v;
                }

                if (value != 0)
                {
                    buffer[--itr] = (byte)('0' + value);
                }

                return offset + lg10;
            }
            else
            {
                buffer[offset++] = (byte)('0' + value);
            }

            return offset;
        }

        #endregion

        // Finds the ends of each token in the argument buffer.
        private int[] argEnds = new int[5];
        private int setMsgArgsAryOffsets(byte[] buffer, long length)
        {
            if (convertToStrBuf.Length < length)
            {
                convertToStrBuf = new char[length];
            }

            int count = 0;
            int i = 0;

            // We support up to 5 elements in this protocol version
            for ( ; i < length && count < 5; i++)
            {
                convertToStrBuf[i] = (char)buffer[i];
                if (buffer[i] == ' ')
                {
                    argEnds[count] = i;
                    count++;
                }
            }

            argEnds[count] = i;
            count++;

            return count;
        }

        // Here we go ahead and convert the message args into
        // strings, numbers, etc.  The msgArg object is a temporary
        // place to hold them, until we create the message.
        //
        // These strings, once created, are never copied.
        internal void processHeaderMsgArgs(byte[] buffer, long length)
        {
            int argCount = setMsgArgsAryOffsets(buffer, length);

            switch (argCount)
            {
                case 4:
                    msgArgs.subject = new string(convertToStrBuf, 0, argEnds[0]);
                    msgArgs.sid = ToInt64(buffer, argEnds[0] + 1, argEnds[1]);
                    msgArgs.reply = null;
                    msgArgs.hdr = (int)ToInt64(buffer, argEnds[1] + 1, argEnds[2]);
                    msgArgs.size = (int)ToInt64(buffer, argEnds[2] + 1, argEnds[3]);
                    break;
                case 5:
                    msgArgs.subject = new string(convertToStrBuf, 0, argEnds[0]);
                    msgArgs.sid = ToInt64(buffer, argEnds[0] + 1, argEnds[1]);
                    msgArgs.reply = new string(convertToStrBuf, argEnds[1] + 1, argEnds[2] - argEnds[1] - 1);
                    msgArgs.hdr = (int)ToInt64(buffer, argEnds[2] + 1, argEnds[3]);
                    msgArgs.size = (int)ToInt64(buffer, argEnds[3] + 1, argEnds[4]);
                    break;
                default:
                    throw new NATSException("Unable to parse message arguments: " + Encoding.UTF8.GetString(buffer, 0, (int)length));
            }

            if (msgArgs.size < 0)
            {
                throw new NATSException("Invalid Message - Bad or Missing Size: " + Encoding.UTF8.GetString(buffer, 0, (int)length));
            }
            if (msgArgs.sid < 0)
            {
                throw new NATSException("Invalid Message - Bad or Missing Sid: " + Encoding.UTF8.GetString(buffer, 0, (int)length));
            }
            if (msgArgs.hdr < 0 || msgArgs.hdr > msgArgs.size)
            {
                throw new NATSException("Invalid Message - Bad or Missing Header Size: " + Encoding.UTF8.GetString(buffer, 0, (int)length));
            }
        }

        // Same as above, except when we know there is no header.
        internal void processMsgArgs(byte[] buffer, long length)
        {
            int argCount = setMsgArgsAryOffsets(buffer, length);
            msgArgs.hdr = 0;

            switch (argCount)
            {
                case 3:
                    msgArgs.subject = new string(convertToStrBuf, 0, argEnds[0]);
                    msgArgs.sid = ToInt64(buffer, argEnds[0] + 1, argEnds[1]);
                    msgArgs.reply = null;
                    msgArgs.size = (int)ToInt64(buffer, argEnds[1] + 1, argEnds[2]);
                    break;
                case 4:
                    msgArgs.subject = new string(convertToStrBuf, 0, argEnds[0]);
                    msgArgs.sid = ToInt64(buffer, argEnds[0] + 1, argEnds[1]);
                    msgArgs.reply = new string(convertToStrBuf, argEnds[1] + 1, argEnds[2] - argEnds[1] - 1);
                    msgArgs.size = (int)ToInt64(buffer, argEnds[2] + 1, argEnds[3]);
                    break;
                default:
                    throw new NATSException("Unable to parse message arguments: " + Encoding.UTF8.GetString(buffer, 0, (int)length));
            }

            if (msgArgs.size < 0)
            {
                throw new NATSException("Invalid Message - Bad or Missing Size: " + Encoding.UTF8.GetString(buffer, 0, (int)length));
            }
            if (msgArgs.sid < 0)
            {
                throw new NATSException("Invalid Message - Bad or Missing Sid: " + Encoding.UTF8.GetString(buffer, 0, (int)length));
            }
        }

        // A simple ToInt64.
        // Assumes: positive integers.
        static long ToInt64(byte[] buffer, int start, int end)
        {
            int length = end - start;
            switch (length)
            {
                case 0:
                    return 0;
                case 1:
                    return buffer[start] - '0';
                case 2:
                    return 10 * (buffer[start] - '0') 
                         + (buffer[start + 1] - '0');
                case 3:
                    return 100 * (buffer[start] - '0')
                         + 10 * (buffer[start + 1] - '0')
                         + (buffer[start + 2] - '0');
                case 4:
                    return 1000 * (buffer[start] - '0')
                         + 100 * (buffer[start + 1] - '0')
                         + 10 * (buffer[start + 2] - '0')
                         + (buffer[start + 3] - '0');
                case 5:
                    return 10000 * (buffer[start] - '0')
                         + 1000 * (buffer[start + 1] - '0')
                         + 100 * (buffer[start + 2] - '0')
                         + 10 * (buffer[start + 3] - '0')
                         + (buffer[start + 4] - '0');
                case 6:
                    return 100000 * (buffer[start] - '0')
                         + 10000 * (buffer[start + 1] - '0')
                         + 1000 * (buffer[start + 2] - '0')
                         + 100 * (buffer[start + 3] - '0')
                         + 10 * (buffer[start + 4] - '0')
                         + (buffer[start + 5] - '0');
                default:
                    if (length < 0)
                        throw new ArgumentOutOfRangeException("end");
                    break;
            }

            long value = 0L;

            int i = start;
            while (i < end)
            {
                value *= 10L;
                value += (buffer[i++] - '0');
            }

            return value;
        }


        // processMsg is called by parse and will place the msg on the
        // appropriate channel for processing. All subscribers have their
        // their own channel. If the channel is full, the connection is
        // considered a slow subscriber.
        internal void processMsg(byte[] msgBytes, long length)
        {
            bool maxReached = false;
            Subscription s;

            lock (mu)
            {
                stats.inMsgs++;
                stats.inBytes += length;

                if (!subs.TryGetValue(msgArgs.sid, out s))
                    return; // this can happen when a subscriber is unsubscribing.

                lock (s.mu)
                {
                    maxReached = s.tallyMessage(length);
                    if (maxReached == false)
                    {
                        Msg msg = msgArgs.reply != null && msgArgs.reply.StartsWith(JetStreamConstants.JsAckSubjectPrefix)
                            ? new JetStreamMsg(this, msgArgs, s, msgBytes, length)
                            : new Msg(msgArgs, s, msgBytes, length);
                        
                        s.addMessage(msg, opts.subChanLen);
                    } // maxreached == false

                } // lock s.mu

                if (maxReached)
                    removeSub(s);

            } // lock conn.mu
        }

        // processSlowConsumer will set SlowConsumer state and fire the
        // async error handler if registered.
        internal void processSlowConsumer(Subscription s)
        {
            lastEx = new NATSSlowConsumerException();
            if (!s.sc)
            {
                EventHandler<ErrEventArgs> aseh = opts.AsyncErrorEventHandler;
                if (aseh != null)
                {
                    callbackScheduler.Add(
                        () => { aseh(this, new ErrEventArgs(this, s, "Slow Consumer")); }
                    );
                }
            }
            s.sc = true;
        }

        private void kickFlusher()
        {
            lock (flusherLock)
            {
                if (!flusherKicked)
                    Monitor.Pulse(flusherLock);

                flusherKicked = true;
            }
        }

        private bool waitForFlusherKick()
        {
            lock (flusherLock)
            {
                if (flusherDone == true)
                    return false;

                // if kicked before we get here meantime, skip
                // waiting.
                if (!flusherKicked)
                {
                    // A spurious wakeup here is OK - we'll just do
                    // an earlier flush.
                    Monitor.Wait(flusherLock);
                }

                flusherKicked = false;
            }

            return true;
        }

        private void setFlusherDone(bool value)
        {
            lock (flusherLock)
            {
                flusherDone = value;

                if (flusherDone)
                    kickFlusher();
            }
        }

        private bool isFlusherDone()
        {
            lock (flusherLock)
            {
                return flusherDone;
            }
        }

        // flusher is a separate task that will process flush requests for the write
        // buffer. This allows coalescing of writes to the underlying socket.
        private void flusher()
        {
            setFlusherDone(false);

            if (conn.Connected == false)
            {
                return;
            }

            while (!isFlusherDone())
            {
                bool val = waitForFlusherKick();
                if (val == false)
                    return;

                lock (mu)
                {
                    if (!isConnected())
                        return;

                    if (bw.CanWrite)
                    {
                        try
                        {
                            bw.Flush();
                        }
                        catch (Exception) {  /* ignore */ }
                    }
                }

                // Yield for a millisecond.  This reduces resource contention,
                // increasing throughput by about 50%.
                Thread.Sleep(1);
            }
        }

        // processPing will send an immediate pong protocol response to the
        // server. The server uses this mechanism to detect dead clients.
        internal void processPing()
        {
            sendProto(PONG_P_BYTES, PONG_P_BYTES_LEN);
        }

        // processPong is used to process responses to the client's ping
        // messages. We use pings for the flush mechanism as well.
        internal void processPong()
        { 
            if (pongs.TryDequeue(out var ch))
                ch?.add(true);

            Interlocked.Exchange(ref pout, 0);
        }

        // processOK is a placeholder for processing OK messages.
        internal void processOK()
        {
            // NOOP;
            return;
        }

        // processInfo is used to parse the info messages sent
        // from the server.
        // Caller must lock.
        internal void processInfo(string json, bool notify)
        {
            if (json == null || IC._EMPTY_.Equals(json))
            {
                return;
            }

            info = new ServerInfo(json);
            var discoveredUrls = info.ConnectURLs;

            // The discoveredUrls array could be empty/not present on initial
            // connect if advertise is disabled on that server, or servers that
            // did not include themselves in the async INFO protocol.
            // If empty, do not remove the implicit servers from the pool.  

            // Note about pool randomization: when the pool was first created,
            // it was randomized (if allowed). We keep the order the same (removing
            // implicit servers that are no longer sent to us). New URLs are sent
            // to us in no specific order so don't need extra randomization.
            if (discoveredUrls != null && discoveredUrls.Length > 0)
            {
                // Prune out implicit servers no longer needed.  
                // The Add in srvPool is idempotent, so just add
                // the entire list.
                srvPool.PruneOutdatedServers(discoveredUrls);
                var serverAdded = srvPool.Add(discoveredUrls, true);
                if (notify && serverAdded)
                {
                    scheduleConnEvent(opts.ServerDiscoveredEventHandler);
                }
            }

            if (notify && info.LameDuckMode && opts.LameDuckModeEventHandler != null)
            {
                scheduleConnEvent(opts.LameDuckModeEventHandler);
            }
        }

        internal void processAsyncInfo(byte[] jsonBytes, int length)
        {
            lock (mu)
            {
                processInfo(Encoding.UTF8.GetString(jsonBytes, 0, length), true);
            }
        }

        /// <summary>
        /// Gets the last <see cref="Exception"/> encountered by this instance,
        /// otherwise <c>null</c>.
        /// </summary>
        public Exception LastError
        {
            get
            {
                return this.lastEx;
            }
        }

        // processErr processes any error messages from the server and
        // sets the connection's lastError.
        internal void processErr(MemoryStream errorStream)
        {
            bool invokeDelegates = false;
            Exception ex = null;

            string s = getNormalizedError(errorStream);

            if (IC.STALE_CONNECTION.Equals(s))
            {
                processOpError(new NATSStaleConnectionException());
            }
            else if (IC.AUTH_TIMEOUT.Equals(s))
            {
                // Protect against a timing issue where an authoriztion error
                // is handled before the connection close from the server.
                // This can happen in reconnect scenarios.
                processOpError(new NATSConnectionException(IC.AUTH_TIMEOUT));
            }
            else
            {
                ex = new NATSException("Error from processErr(): " + s);
                lock (mu)
                {
                    lastEx = ex;

                    if (status != ConnState.CONNECTING)
                    {
                        invokeDelegates = true;
                    }
                }

                close(ConnState.CLOSED, invokeDelegates, ex);
            }
        }

        // getNormalizedError extracts a string from a MemoryStream, then normalizes it
        // by removing leading and trailing spaces and quotes, and converting to lowercase
        private string getNormalizedError(MemoryStream errorStream)
        {
            string s = Encoding.UTF8.GetString(errorStream.ToArray(), 0, (int)errorStream.Position);
            return s.Trim('\'', '"', ' ', '\t', '\r', '\n').ToLower();
        }

        // Use low level primitives to build the protocol for the publish
        // message.
        //
        // HPUB<SUBJ>[REPLY] <HDR_LEN> <TOT_LEN>
        // <HEADER><PAYLOAD>
        // For example:
        // HPUB SUBJECT REPLY 23 30␍␊NATS/1.0␍␊Header: X␍␊␍␊PAYLOAD␍␊
        // HPUB SUBJECT REPLY 23 23␍␊NATS/1.0␍␊Header: X␍␊␍␊␍␊
        private void WriteHPUBProto(byte[] dst, string subject, string reply, int headerSize, int msgSize,
            out int length, out int offset)
        {
            // skip past the predefined "HPUB "
            int index = HPUB_P_BYTES_LEN;

            // Subject
            index = writeStringToBuffer(dst, index, subject);

            if (reply != null)
            {
                // " REPLY"
                dst[index] = (byte)' ';
                index++;

                index = writeStringToBuffer(dst, index, reply);
            }

            // " HEADERSIZE"
            dst[index] = (byte)' ';
            index++;
            index = writeInt32ToBuffer(dst, index, headerSize);

            // " TOTALSIZE"
            dst[index] = (byte)' ';
            index++;
            index = writeInt32ToBuffer(dst, index, msgSize+headerSize);

            // "\r\n"
            dst[index] = CRLF_BYTES[0];
            dst[index + 1] = CRLF_BYTES[1];
            if (CRLF_BYTES_LEN > 2)
            {
                for (int i = 2; i < CRLF_BYTES_LEN; ++i)
                    dst[index + i] = CRLF_BYTES[i];
            }
            index += CRLF_BYTES_LEN;

            length = index;
            offset = 0;
        }

        // The pubProtoBuf will always start with HPUB... with the
        // rest of the protocol representing a standard publish or
        // header publish.  If a standard publish, we must skip the H
        // to create a PUB protocol message, and reduce the length.
        // We write as if it's a HPUB, but adjust the returned offset
        // and length so the future write will ignore the H.
        private void WritePUBProto(byte[] dst, string subject, string reply, int msgSize,
            out int length, out int offset)
        {
            // skip past the predefined "HPUB "
            int index = HPUB_P_BYTES_LEN;

            // Subject
            index = writeStringToBuffer(dst, index, subject);

            if (reply != null)
            {
                // " REPLY"
                dst[index] = (byte)' ';
                index++;

                index = writeStringToBuffer(dst, index, reply);
            }

            // " "
            dst[index] = (byte)' ';
            index++;

            // " SIZE"
            index = writeInt32ToBuffer(dst, index, msgSize);

            // "\r\n"
            dst[index] = CRLF_BYTES[0];
            dst[index + 1] = CRLF_BYTES[1];
            if (CRLF_BYTES_LEN > 2)
            {
                for (int i = 2; i < CRLF_BYTES_LEN; ++i)
                    dst[index + i] = CRLF_BYTES[i];
            }
            index += CRLF_BYTES_LEN;

            length = index-1;
            offset = 1;
        }

        // publish is the internal function to publish messages to a nats-server.
        // Sends a protocol data message by queueing into the bufio writer
        // and kicking the flush go routine. These writes should be protected.
        internal void publish(string subject, string reply, byte[] headers, byte[] data, int offset, int count, bool flushBuffer)
        {
            if (string.IsNullOrWhiteSpace(subject))
            {
                throw new NATSBadSubscriptionException();
            }
            else if (offset < 0)
            {
                throw new ArgumentOutOfRangeException("offset");
            }
            else if (count < 0)
            {
                throw new ArgumentOutOfRangeException("count");
            }
            else if (data != null && data.Length - offset < count)
            {
                throw new ArgumentException("Invalid offset and count for supplied data");
            }

            lock (mu)
            {
                if (isClosed())
                    throw new NATSConnectionClosedException();

                if (isDrainingPubs())
                    throw new NATSConnectionDrainingException();

                // Proactively reject payloads over the threshold set by server.
                if (count > info.MaxPayload)
                    throw new NATSMaxPayloadException();

                if (lastEx != null)
                    throw lastEx;

                ensurePublishProtocolBuffer(subject, reply);

                // protoLen and protoOffset provide a length and offset for
                // copying the publish protocol buffer.  This is to avoid extra
                // copies or use of multiple publish protocol buffers.
                int protoLen;
                int protoOffset;
                if (headers != null)
                {
                    if (!info.HeadersSupported)
                    {
                        throw new NATSNotSupportedException("Headers are not supported by the server.");
                    }
                    WriteHPUBProto(pubProtoBuf, subject, reply, headers.Length, count, out protoLen, out protoOffset);
                }
                else
                {
                    WritePUBProto(pubProtoBuf, subject, reply, count, out protoLen, out protoOffset);
                }

                // Check if we are reconnecting, and if so check if
                // we have exceeded our reconnect outbound buffer limits.
                // Don't use IsReconnecting to avoid locking.
                if (status == ConnState.RECONNECTING)
                {
                    int rbsize = opts.ReconnectBufferSize;
                    if (rbsize != 0)
                    {
                        if (rbsize == -1)
                            throw new NATSReconnectBufferException("Reconnect buffering has been disabled.");

                        if (flushBuffer)
                            bw.Flush();
                        else
                            kickFlusher();

                        if (pending != null && bw.Position + count + protoLen > rbsize)
                            throw new NATSReconnectBufferException("Reconnect buffer exceeded.");
                    }
                }

                bw.Write(pubProtoBuf, protoOffset, protoLen);

                if (headers != null)
                {
                    bw.Write(headers, 0, headers.Length);
                    stats.outBytes += headers.Length;
                }

                if (count > 0)
                {
                    bw.Write(data, offset, count);
                }

                bw.Write(CRLF_BYTES, 0, CRLF_BYTES_LEN);

                stats.outMsgs++;
                stats.outBytes += count;

                if (flushBuffer)
                {
                    bw.Flush();
                }
                else
                {
                    kickFlusher();
                }
            }

        } // publish

        /// <summary>
        /// Publishes <paramref name="data"/> to the given <paramref name="subject"/>.
        /// </summary>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the data to publish
        /// to the connected NATS server.</param>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is 
        /// <c>null</c> or entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="data"/> exceeds the maximum payload size
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call
        /// while publishing. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public void Publish(string subject, byte[] data)
        {
            int count = data != null ? data.Length : 0;
            publish(subject, null, null, data, 0, count, false);
        }

        /// <summary>
        /// Publishes a sequence of bytes from <paramref name="data"/> to the given <paramref name="subject"/>.
        /// </summary>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the data to publish
        /// to the connected NATS server.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="data"/> at which to begin publishing
        /// bytes to the subject.</param>
        /// <param name="count">The number of bytes to be published to the subject.</param>
        /// <seealso cref="IConnection.Publish(string, byte[])"/>
        public void Publish(string subject, byte[] data, int offset, int count)
        {
            publish(subject, null, null, data, offset, count, false);
        }

        /// <summary>
        /// Publishes a <see cref="Msg"/> instance, which includes the subject, an optional reply, and an
        /// optional data field.
        /// </summary>
        /// <param name="msg">A <see cref="Msg"/> instance containing the subject, optional reply, and data to publish
        /// to the NATS server.</param>
        /// <exception cref="ArgumentNullException"><paramref name="msg"/> is <c>null</c>.</exception>
        /// <exception cref="NATSBadSubscriptionException">The <see cref="Msg.Subject"/> property of
        /// <paramref name="msg"/> is <c>null</c> or entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException">The <see cref="Msg.Data"/> property of <paramref name="msg"/> 
        /// exceeds the maximum payload size supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call 
        /// while publishing. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public void Publish(Msg msg)
        {
            if (msg == null)
            {
                throw new ArgumentNullException("msg");
            }

            int count = msg.Data != null ? msg.Data.Length : 0;
            byte[] headers = msg.header != null ? msg.header.ToByteArray() : null;
            publish(msg.Subject, msg.Reply, headers, msg.Data, 0, count, false);
        }

        /// <summary>
        /// Publishes <paramref name="data"/> to the given <paramref name="subject"/>.
        /// </summary>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="reply">An optional reply subject.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the data to publish
        /// to the connected NATS server.</param>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is <c>null</c> or
        /// entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="data"/> exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call
        /// while publishing. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public void Publish(string subject, string reply, byte[] data)
        {
            int count = data != null ? data.Length : 0;
            publish(subject, reply, null, data, 0, count, false);
        }

        /// <summary>
        /// Publishes a sequence of bytes from <paramref name="data"/> to the given <paramref name="subject"/>.
        /// </summary>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="reply">An optional reply subject.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the data to publish
        /// to the connected NATS server.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="data"/> at which to begin publishing
        /// bytes to the subject.</param>
        /// <param name="count">The number of bytes to be published to the subject.</param>
        /// <seealso cref="IConnection.Publish(string, byte[])"/>
        public void Publish(string subject, string reply, byte[] data, int offset, int count)
        {
            publish(subject, reply, null, data, offset, count, false);
        }

        protected Msg request(string subject, byte[] headers, byte[] data, int offset, int count, int timeout) => requestSync(subject, headers, data, offset, count, timeout);

        private void RemoveOutstandingRequest(string requestId)
        {
            lock (mu)
            {
                waitingRequests.Remove(requestId);
            }
        }

        private static bool IsNoRespondersMsg(Msg m)
        {
            return m != null && m.HasStatus && m.Status.IsNoResponders();
        }

        private void RequestResponseHandler(object sender, MsgHandlerEventArgs args)
        {
            InFlightRequest request;
            bool isClosed;

            if (args.Message == null)
                return;

            var subject = args.Message.Subject;

            lock (mu)
            {
                // if it's a typical response, process normally.
                if (subject.StartsWith(globalRequestInbox))
                {
                    //               \
                    //               \/
                    //  _INBOX.<nuid>.<requestId>
                    var requestId = subject.Substring(globalRequestInbox.Length + 1);
                    if (!waitingRequests.TryGetValue(requestId, out request))
                        return;
                }
                else
                {
                    // We have a jetstream subject (remapped), so if there's only one
                    // request assume we're OK and handle it.
                    if (waitingRequests.Count == 1)
                    {
                        InFlightRequest[] values = new InFlightRequest[1];
                        waitingRequests.Values.CopyTo(values, 0);
                        request = values[0];

                    }
                    else
                    {
                        // if we get here, we have multiple outsanding jetstream
                        // requests.  We can't tell which is which we'll punt.
                        return;
                    }
                }

                isClosed = this.isClosed();
            }

            if (!isClosed)
            {
                if (IsNoRespondersMsg(args.Message))
                {
                    request.Waiter.TrySetException(new NATSNoRespondersException());
                }
                else
                {
                    request.Waiter.TrySetResult(args.Message);
                }
            }
            else
            {
                request.Waiter.TrySetCanceled();
            }
            request.Dispose();
        }

        private InFlightRequest setupRequest(int timeout, CancellationToken token)
        {
            var requestId = Interlocked.Increment(ref nextRequestId);
            if (requestId < 0) //Check if recycled
                requestId = (requestId + long.MaxValue + 1);

            var request = new InFlightRequest(requestId.ToString(CultureInfo.InvariantCulture), token, timeout, RemoveOutstandingRequest);
            request.Waiter.Task.ContinueWith(t => GC.KeepAlive(t.Exception), TaskContinuationOptions.OnlyOnFaulted);

            lock (mu)
            {
                // We shouldn't ever get an Argument exception because the ID is incrementing
                // and since this is performant sensitive code, skipping an existence check.
                waitingRequests.Add(request.Id, request);

                if (globalRequestSubscription != null)
                    return request;

                if (globalRequestSubscription == null)
                    globalRequestSubscription = subscribeAsync(string.Concat(globalRequestInbox, ".*"), null,
                        RequestResponseHandler);
            }

            return request;
        }

        internal Msg requestSync(string subject, byte[] headers, byte[] data, int offset, int count, int timeout)
        {
            if (string.IsNullOrWhiteSpace(subject))
                throw new NATSBadSubscriptionException();
            
            // a timeout of 0 will never succeed - do not allow it.
            if (timeout == 0)
                throw new ArgumentException("Timeout must not be 0.", nameof(timeout));

            // offset/count checking covered by publish
            Msg m;
            if (opts.UseOldRequestStyle)
            {
                m = oldRequest(subject, headers, data, offset, count, timeout);
            }
            else
            {

                using (var request = setupRequest(timeout, CancellationToken.None))
                {
                    request.Token.ThrowIfCancellationRequested();

                    publish(subject, string.Concat(globalRequestInbox, ".", request.Id), headers, data, offset, count, true);

                    m = request.Waiter.Task.GetAwaiter().GetResult();
                }
            }

            if (IsNoRespondersMsg(m))
            {
                throw new NATSNoRespondersException();
            }

            return m;
        }

        private async Task<Msg> requestAsync(string subject, byte[] headers, byte[] data, int offset, int count, int timeout, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(subject))
                throw new NATSBadSubscriptionException();

            // a timeout of 0 will never succeed - do not allow it.
            if (timeout == 0)
                throw new ArgumentException("Timeout must not be 0.", nameof(timeout));

            // offset/count checking covered by publish

            if (opts.UseOldRequestStyle)
                return await oldRequestAsync(subject, headers, data, offset, count, timeout, token).ConfigureAwait(false);

            using (var request = setupRequest(timeout, token))
            {
                request.Token.ThrowIfCancellationRequested();

                publish(subject, string.Concat(globalRequestInbox, ".", request.Id), headers, data, offset, count, true);

                // InFlightRequest links the token cancellation
                return await request.Waiter.Task.ConfigureAwait(false);
            }
        }

        private Msg oldRequest(string subject, byte[] headers, byte[] data, int offset, int count, int timeout)
        {
            var inbox = NewInbox();

            using (var s = subscribeSync(inbox, null))
            {
                s.AutoUnsubscribe(1);

                publish(subject, inbox, headers, data, offset, count, true);
                var m = s.NextMessage(timeout);
                s.unsubscribe(false);

                if (IsNoRespondersMsg(m))
                {
                    throw new NATSNoRespondersException();
                }

                return m;
            }
        }

        /// <summary>
        /// Sends a request payload and returns the response <see cref="Msg"/>, or throws
        /// <see cref="NATSTimeoutException"/> if the <paramref name="timeout"/> expires.
        /// </summary>
        /// <remarks>
        /// <see cref="Request(string, byte[])"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="Connection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        /// <returns>A <see cref="Msg"/> with the response from the NATS server.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is less than or equal to zero 
        /// (<c>0</c>).</exception>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is <c>null</c> or
        /// entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="data"/> exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the 
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call
        /// while executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Msg Request(string subject, byte[] data, int timeout)
        {
            int count = data != null ? data.Length : 0;
            return request(subject, null, data, 0, count, timeout);
        }

        /// <summary>
        /// Sends a sequence of bytes as the request payload and returns the response <see cref="Msg"/>, or throws
        /// <see cref="NATSTimeoutException"/> if the <paramref name="timeout"/> expires.
        /// </summary>
        /// <remarks>
        /// <see cref="Request(string, byte[], int, int, int)"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="Connection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="data"/> at which to begin publishing
        /// bytes to the subject.</param>
        /// <param name="count">The number of bytes to be published to the subject.</param>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        /// <returns>A <see cref="Msg"/> with the response from the NATS server.</returns>
        /// <seealso cref="IConnection.Request(string, byte[])"/>
        public Msg Request(string subject, byte[] data, int offset, int count, int timeout)
        {
            return request(subject, null, data, offset, count, timeout);
        }

        /// <summary>
        /// Sends a request payload and returns the response <see cref="Msg"/>.
        /// </summary>
        /// <remarks>
        /// <see cref="Request(string, byte[])"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="Connection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <returns>A <see cref="Msg"/> with the response from the NATS server.</returns>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is <c>null</c> or 
        /// entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="data"/> exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Msg Request(string subject, byte[] data)
        {
            int count = data != null ? data.Length : 0;
            return request(subject, null, data, 0, count, Timeout.Infinite);
        }

        /// <summary>
        /// Sends a sequence of bytes as the request payload and returns the response <see cref="Msg"/>.
        /// </summary>
        /// <remarks>
        /// <para>NATS supports two flavors of request-reply messaging: point-to-point or one-to-many. Point-to-point
        /// involves the fastest or first to respond. In a one-to-many exchange, you set a limit on the number of 
        /// responses the requestor may receive and instead must use a subscription (<see cref="ISubscription.AutoUnsubscribe(int)"/>).
        /// In a request-response exchange, publish request operation publishes a message with a reply subject expecting
        /// a response on that reply subject.</para>
        /// <para><see cref="Request(string, byte[], int, int)"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="Connection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.</para>
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <exception cref="NATSMaxPayloadException"><paramref name="data"/> exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <param name="offset">The zero-based byte offset in <paramref name="data"/> at which to begin publishing
        /// bytes to the subject.</param>
        /// <param name="count">The number of bytes to be published to the subject.</param>
        /// <returns>A <see cref="Msg"/> with the response from the NATS server.</returns>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is <c>null</c> or 
        /// entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="data"/> exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Msg Request(string subject, byte[] data, int offset, int count)
        {
            return request(subject, null, data, offset, count, Timeout.Infinite);
        }

        /// <summary>
        /// Sends a request payload and returns the response <see cref="Msg"/>, or throws
        /// <see cref="NATSTimeoutException"/> if the <paramref name="timeout"/> expires.
        /// </summary>
        /// <remarks>
        /// <see cref="Request(Msg)"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="Connection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="message">A NATS <see cref="Msg"/> that contains the request data to publish
        /// to the connected NATS server.  The reply subject will be overridden.</param>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        /// <returns>A <see cref="Msg"/> with the response from the NATS server.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is less than or equal to zero 
        /// (<c>0</c>).</exception>
        /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
        /// <exception cref="NATSBadSubscriptionException">The <paramref name="message"/> subject is <c>null</c>
        /// or entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="message"/> payload exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the 
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call
        /// while executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Msg Request(Msg message, int timeout)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            byte[] headerBytes = message.header?.ToByteArray();
            byte[] data = message.Data;
            int count = data != null ? data.Length : 0;
            return request(message.Subject, headerBytes, data, 0, count, timeout);
        }

        /// <summary>
        /// Sends a request payload and returns the response <see cref="Msg"/>.
        /// </summary>
        /// <remarks>
        /// <see cref="Request(Msg)"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="Connection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="message">A NATS <see cref="Msg"/> that contains the request data to publish
        /// to the connected NATS server.  The reply subject will be overridden.</param>
        /// <returns>A <see cref="Msg"/> with the response from the NATS server.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
        /// <exception cref="NATSBadSubscriptionException">The <paramref name="message"/> subject is <c>null</c>
        /// or entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="message"/> payload exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Msg Request(Msg message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            byte[] headerBytes = message.header?.ToByteArray();
            byte[] data = message.Data;
            int count = data != null ? data.Length : 0;
            return request(message.Subject, headerBytes, data, 0, count, Timeout.Infinite);
        }


        private Task<Msg> oldRequestAsync(string subject, byte[] headers, byte[] data, int offset, int count, int timeout, CancellationToken ct)
        {
            // Simple case without a cancellation token.
            if (ct == CancellationToken.None)
                return Task.Run(() => oldRequest(subject, headers, data, offset, count, timeout), ct);

            // More complex case, supporting cancellation.
            return Task.Run(() =>
            {
                // check if we are already cancelled.
                ct.ThrowIfCancellationRequested();

                Msg m = null;
                string inbox = NewInbox();

                // Use synchronous subscriber to minimize overhead.
                // An async subscriber would be easier, but creates 
                // yet another task internally.  The cost is it could
                // take up to CANCEL_IVL ms to cancel.
                SyncSubscription s = subscribeSync(inbox, null);
                s.AutoUnsubscribe(1);

                publish(subject, inbox, headers, data, offset, count, true);

                int timeRemaining = timeout;

                while (m == null && ct.IsCancellationRequested == false)
                {
                    if (timeout == Timeout.Infinite)
                    {
                        // continue in a loop until cancellation
                        // is requested.
                        try
                        {
                            m = s.NextMessage(REQ_CANCEL_IVL);
                        }
                        catch (NATSTimeoutException) { /* ignore */ };
                    }
                    else
                    {
                        int waitTime = timeRemaining < REQ_CANCEL_IVL ?
                            timeRemaining : REQ_CANCEL_IVL;

                        try
                        {
                            m = s.NextMessage(waitTime);
                        }
                        catch (NATSTimeoutException)
                        {
                            timeRemaining -= waitTime;
                            if (timeRemaining <= 0)
                            {
                                s.unsubscribe(false);
                                throw;
                            }
                        }

                        if (IsNoRespondersMsg(m))
                        {
                            throw new NATSNoRespondersException();
                        }
                    }
                }

                // we either have a message or have been cancelled.
                s.unsubscribe(false);

                // Throw if cancelled.  Note, the cancellation occured
                // after we've received a message, go ahead and honor 
                // the cancellation.
                ct.ThrowIfCancellationRequested();

                return m;

            }, ct);
        }

        /// <summary>
        /// Asynchronously sends a request payload and returns the response <see cref="Msg"/>, or throws 
        /// <see cref="NATSTimeoutException"/> if the <paramref name="timeout"/> expires.
        /// </summary>
        /// <remarks>
        /// <see cref="RequestAsync(string, byte[], int)"/> will create an unique inbox for this request, sharing a
        /// single subscription for all replies to this <see cref="Connection"/> instance. However, if
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription.
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        /// <returns>A task that represents the asynchronous read operation. The value of the <see cref="Task{TResult}.Result"/>
        /// parameter contains a <see cref="Msg"/> with the response from the NATS server.</returns>
        /// <exception cref="ArgumentException"><paramref name="timeout"/> is less than or equal to zero 
        /// (<c>0</c>).</exception>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is <c>null</c>
        /// or entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="data"/> exceeds the maximum payload size
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call
        /// while executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before
        /// it could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(string subject, byte[] data, int timeout)
        {
            int count = data != null ? data.Length : 0;
            return requestAsync(subject, null, data, 0, count, timeout, CancellationToken.None);
        }

        /// <summary>
        /// Asynchronously sends a sequence of bytes as the request payload and returns the response <see cref="Msg"/>, or throws 
        /// <see cref="NATSTimeoutException"/> if the <paramref name="timeout"/> expires.
        /// </summary>
        /// <remarks>
        /// <see cref="RequestAsync(string, byte[], int, int, int)"/> will create an unique inbox for this request, sharing a
        /// single subscription for all replies to this <see cref="Connection"/> instance. However, if
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription.
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="data"/> at which to begin publishing
        /// bytes to the subject.</param>
        /// <param name="count">The number of bytes to be published to the subject.</param>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        /// <returns>A task that represents the asynchronous read operation. The value of the <see cref="Task{TResult}.Result"/>
        /// parameter contains a <see cref="Msg"/> with the response from the NATS server.</returns>
        /// <seealso cref="IConnection.Request(string, byte[])"/>
        public Task<Msg> RequestAsync(string subject, byte[] data, int offset, int count, int timeout)
        {
            return requestAsync(subject, null, data, offset, count, timeout, CancellationToken.None);
        }

        /// <summary>
        /// Asynchronously sends a request payload and returns the response <see cref="Msg"/>.
        /// </summary>
        /// <remarks>
        /// <see cref="RequestAsync(string, byte[])"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="Connection"/> instance. However, if
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <returns>A task that represents the asynchronous read operation. The value of the 
        /// <see cref="Task{TResult}.Result"/> parameter contains a <see cref="Msg"/> with the response from the NATS
        /// server.</returns>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is <c>null</c> or
        /// entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="data"/> exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before
        /// it could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(string subject, byte[] data)
        {
            int count = data != null ? data.Length : 0;
            return requestAsync(subject, null, data, 0, count, Timeout.Infinite, CancellationToken.None);
        }

        /// <summary>
        /// Asynchronously sends a sequence of bytes as the request payload and returns the response <see cref="Msg"/>.
        /// </summary>
        /// <remarks>
        /// <see cref="RequestAsync(string, byte[], int, int)"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="Connection"/> instance. However, if
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="data"/> at which to begin publishing
        /// bytes to the subject.</param>
        /// <param name="count">The number of bytes to be published to the subject.</param>
        /// <returns>A task that represents the asynchronous read operation. The value of the 
        /// <see cref="Task{TResult}.Result"/> parameter contains a <see cref="Msg"/> with the response from the NATS
        /// server.</returns>
        /// <seealso cref="IConnection.Request(string, byte[])"/>
        public Task<Msg> RequestAsync(string subject, byte[] data, int offset, int count)
        {
            return requestAsync(subject, null, data, offset, count, Timeout.Infinite, CancellationToken.None);
        }

        /// <summary>
        /// Asynchronously sends a request payload and returns the response <see cref="Msg"/>, or throws
        /// <see cref="NATSTimeoutException"/> if the <paramref name="timeout"/> expires, while monitoring for 
        /// cancellation requests.
        /// </summary>
        /// <remarks>
        /// <see cref="RequestAsync(string, byte[], int, CancellationToken)"/> will create an unique inbox for this
        /// request, sharing a single subscription for all replies to this <see cref="Connection"/> instance. However,
        /// if <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription.
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        /// <param name="token">The token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous read operation. The value of the
        /// <see cref="Task{TResult}.Result"/> parameter contains  a <see cref="Msg"/> with the response from the NATS
        /// server.</returns>
        /// <exception cref="ArgumentException"><paramref name="timeout"/> is less than or equal to zero
        /// (<c>0</c>).</exception>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is <c>null</c> or
        /// entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="data"/> exceeds the maximum payload size
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before
        /// it could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(string subject, byte[] data, int timeout, CancellationToken token)
        {
            int count = data != null ? data.Length : 0;
            return requestAsync(subject, null, data, 0, count, timeout, token);
        }

        /// <summary>
        /// Asynchronously sends a request payload and returns the response <see cref="Msg"/>, while monitoring for
        /// cancellation requests.
        /// </summary>
        /// <remarks>
        /// <see cref="RequestAsync(string, byte[], CancellationToken)"/> will create an unique inbox for this request,
        /// sharing a single subscription for all replies to this <see cref="Connection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription.
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <param name="token">The token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous read operation. The value of the
        /// <see cref="Task{TResult}.Result"/> parameter contains a <see cref="Msg"/> with the response from the NATS 
        /// server.</returns>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is <c>null</c>
        /// or entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="data"/> exceeds the maximum payload size supported
        /// by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the 
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while 
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before it
        /// could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(string subject, byte[] data, CancellationToken token)
        {
            int count = data != null ? data.Length : 0;
            return requestAsync(subject, null, data, 0, count, Timeout.Infinite, token);
        }

        /// <summary>
        /// Asynchronously sends a sequence of bytes as the request payload and returns the response <see cref="Msg"/>,
        /// while monitoring for cancellation requests.
        /// </summary>
        /// <remarks>
        /// <see cref="RequestAsync(string, byte[], int, int, CancellationToken)"/> will create an unique inbox for this request,
        /// sharing a single subscription for all replies to this <see cref="Connection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription.
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="Byte"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="data"/> at which to begin publishing
        /// bytes to the subject.</param>
        /// <param name="count">The number of bytes to be published to the subject.</param>
        /// <param name="token">The token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous read operation. The value of the
        /// <see cref="Task{TResult}.Result"/> parameter contains a <see cref="Msg"/> with the response from the NATS 
        /// server.</returns>
        /// <seealso cref="IConnection.Request(string, byte[])"/>
        public Task<Msg> RequestAsync(string subject, byte[] data, int offset, int count, CancellationToken token)
        {
            return requestAsync(subject, null, data, offset, count, Timeout.Infinite, token);
        }

        /// <summary>
        /// Asynchronously sends a request message and returns the response <see cref="Msg"/>, or throws 
        /// <see cref="NATSTimeoutException"/> if the <paramref name="timeout"/> expires.
        /// </summary>
        /// <remarks>
        /// <see cref="RequestAsync(Msg, int)"/> will create an unique inbox for this request, sharing a
        /// single subscription for all replies to this <see cref="Connection"/> instance. However, if
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription.
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="message">A NATS <see cref="Msg"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        /// <returns>A task that represents the asynchronous read operation. The value of the <see cref="Task{TResult}.Result"/>
        /// parameter contains a <see cref="Msg"/> with the response from the NATS server.</returns>
        /// <exception cref="ArgumentException"><paramref name="timeout"/> is less than or equal to zero 
        /// (<c>0</c>).</exception>
        /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
        /// <exception cref="NATSBadSubscriptionException">The <paramref name="message"/> subject is <c>null</c>
        /// or entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="message"/> payload exceeds the maximum payload size
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call
        /// while executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before
        /// it could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(Msg message, int timeout)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            byte[] headerBytes = message.header?.ToByteArray();
            byte[] data = message.Data;
            int count = data != null ? data.Length : 0;
            return requestAsync(message.Subject, headerBytes, data, 0, count, timeout, CancellationToken.None);
        }

        /// <summary>
        /// Asynchronously sends a request message and returns the response <see cref="Msg"/>.
        /// </summary>
        /// <remarks>
        /// <see cref="RequestAsync(Msg)"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="Connection"/> instance. However, if
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="message">A NATS <see cref="Msg"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <returns>A task that represents the asynchronous read operation. The value of the 
        /// <see cref="Task{TResult}.Result"/> parameter contains a <see cref="Msg"/> with the response from the NATS
        /// server.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
        /// <exception cref="NATSBadSubscriptionException">The <paramref name="message"/> subject is <c>null</c>
        /// or entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="message"/> payload exceeds the maximum payload size 
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before
        /// it could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(Msg message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            byte[] headerBytes = message.header?.ToByteArray();
            byte[] data = message.Data;
            int count = data != null ? data.Length : 0;
            return requestAsync(message.Subject, headerBytes, data, 0, count, Timeout.Infinite, CancellationToken.None);
        }

        /// <summary>
        /// Asynchronously sends a request message and returns the response <see cref="Msg"/>, or throws
        /// <see cref="NATSTimeoutException"/> if the <paramref name="timeout"/> expires, while monitoring for 
        /// cancellation requests.
        /// </summary>
        /// <remarks>
        /// <see cref="RequestAsync(Msg, int, CancellationToken)"/> will create an unique inbox for this
        /// request, sharing a single subscription for all replies to this <see cref="Connection"/> instance. However,
        /// if <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription.
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="message">A NATS <see cref="Msg"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        /// <param name="token">The token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous read operation. The value of the
        /// <see cref="Task{TResult}.Result"/> parameter contains  a <see cref="Msg"/> with the response from the NATS
        /// server.</returns>
        /// <exception cref="ArgumentException"><paramref name="timeout"/> is less than or equal to zero
        /// (<c>0</c>).</exception>
        /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
        /// <exception cref="NATSBadSubscriptionException">The <paramref name="message"/> subject is <c>null</c>
        /// or entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="message"/> payload exceeds the maximum payload size
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before
        /// it could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(Msg message, int timeout, CancellationToken token)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            byte[] headerBytes = message.header?.ToByteArray();
            byte[] data = message.Data;
            int count = data != null ? data.Length : 0;
            return requestAsync(message.Subject, headerBytes, data, 0, count, timeout, token);
        }

        /// <summary>
        /// Asynchronously sends a request message and returns the response <see cref="Msg"/>, while monitoring for
        /// cancellation requests.
        /// </summary>
        /// <remarks>
        /// <see cref="RequestAsync(Msg, CancellationToken)"/> will create an unique inbox for this request,
        /// sharing a single subscription for all replies to this <see cref="Connection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription.
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="message">A NATS <see cref="Msg"/> that contains the request data to publish
        /// to the connected NATS server.</param>
        /// <param name="token">The token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous read operation. The value of the
        /// <see cref="Task{TResult}.Result"/> parameter contains a <see cref="Msg"/> with the response from the NATS 
        /// server.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
        /// <exception cref="NATSBadSubscriptionException">The <paramref name="message"/> subject is <c>null</c>
        /// or entirely whitespace.</exception>
        /// <exception cref="NATSMaxPayloadException"><paramref name="message"/> payload exceeds the maximum payload size
        /// supported by the NATS server.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the 
        /// response.</exception>
        /// <exception cref="NATSNoRespondersException">No responders are available for this request.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while 
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before it
        /// could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(Msg message, CancellationToken token)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            byte[] headerBytes = message.header?.ToByteArray();
            byte[] data = message.Data;
            int count = data != null ? data.Length : 0;
            return requestAsync(message.Subject, headerBytes, data, 0, count, Timeout.Infinite, token);
        }

        /// <summary>
        /// Creates an inbox string which can be used for directed replies from subscribers.
        /// </summary>
        /// <remarks>
        /// The returned inboxes are guaranteed to be unique, but can be shared and subscribed
        /// to by others.
        /// </remarks>
        /// <returns>A unique inbox string.</returns>
        public string NewInbox()
        {
            var prefix = opts.customInboxPrefix ?? IC.inboxPrefix;
            return prefix + _nuid.GetNext();
        }

        internal void sendSubscriptionMessage(AsyncSubscription s)
        {
            lock (mu)
            {
                // We will send these for all subs when we reconnect
                // so that we can suppress here.
                if (!isReconnecting())
                {
                    writeString(IC.subProto, s.Subject, s.Queue, s.sid.ToNumericString());
                    kickFlusher();
                }
            }
        }

        private void addSubscription(Subscription s)
        {
            s.sid = Interlocked.Increment(ref ssid);
            subs[s.sid] = s;
        }

        // caller must lock
        // See Options.SubscriberDeliveryTaskCount
        private void enableSubChannelPooling()
        {
            if (subChannelPool != null)
                return;

            if (opts.subscriberDeliveryTaskCount > 0)
            {
                subChannelPool = new SubChannelPool(this,
                    opts.subscriberDeliveryTaskCount);
            }
        }

        // caller must lock.
        // See Options.SubscriberDeliveryTaskCount
        private void disableSubChannelPooling()
        {
            if (subChannelPool != null)
            {
                subChannelPool.Dispose();
            }
        }

        internal delegate SyncSubscription CreateSyncSubscriptionDelegate(Connection conn, string subject, string queue);
        internal delegate AsyncSubscription CreateAsyncSubscriptionDelegate(Connection conn, string subject, string queue);

        internal AsyncSubscription subscribeAsync(string subject, string queue,
            EventHandler<MsgHandlerEventArgs> handler, CreateAsyncSubscriptionDelegate createAsyncSubscriptionDelegate = null)
        {
            if (!Subscription.IsValidSubject(subject))
            {
                throw new NATSBadSubscriptionException("Invalid subject.");
            }
            if (queue != null && !Subscription.IsValidQueueGroupName(queue))
            {
                throw new NATSBadSubscriptionException("Invalid queue group name.");
            }

            AsyncSubscription s = null;

            lock (mu)
            {
                if (isClosed())
                    throw new NATSConnectionClosedException();
                if (IsDraining())
                    throw new NATSConnectionDrainingException();

                enableSubChannelPooling();

                s = createAsyncSubscriptionDelegate == null 
                    ? new AsyncSubscription(this, subject, queue)
                    : createAsyncSubscriptionDelegate(this, subject, queue);

                addSubscription(s);

                if (handler != null)
                {
                    s.MessageHandler += handler;
                    s.Start();
                }
            }

            return s;
        }
        
        // subscribe is the internal subscribe 
        // function that indicates interest in a subject.
        internal SyncSubscription subscribeSync(string subject, string queue, 
            CreateSyncSubscriptionDelegate createSyncSubscriptionDelegate = null)
        {
            if (!Subscription.IsValidSubject(subject))
            {
                throw new NATSBadSubscriptionException("Invalid subject.");
            }
            if (queue != null && !Subscription.IsValidQueueGroupName(queue))
            {
                throw new NATSBadSubscriptionException("Invalid queue group name.");
            }

            SyncSubscription s = null;

            lock (mu)
            {
                if (isClosed())
                    throw new NATSConnectionClosedException();
                if (IsDraining())
                    throw new NATSConnectionDrainingException();

                s = createSyncSubscriptionDelegate == null 
                    ? new SyncSubscription(this, subject, queue) 
                    : createSyncSubscriptionDelegate(this, subject, queue);

                addSubscription(s);

                // We will send these for all subs when we reconnect
                // so that we can suppress here.
                if (!isReconnecting())
                {
                    writeString(IC.subProto, subject, queue, s.sid.ToNumericString());
                }
            }

            kickFlusher();

            return s;
        }

        /// <summary>
        /// Expresses interest in the given <paramref name="subject"/> to the NATS Server.
        /// </summary>
        /// <param name="subject">The subject on which to listen for messages.
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <returns>An <see cref="ISyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>.</returns>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is
        /// <c>null</c> or entirely whitespace.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public ISyncSubscription SubscribeSync(string subject)
        {
            return subscribeSync(subject, null);
        }

        /// <summary>
        /// Expresses interest in the given <paramref name="subject"/> to the NATS Server.
        /// </summary>
        /// <remarks>
        /// The <see cref="IAsyncSubscription"/> returned will not start receiving messages until
        /// <see cref="IAsyncSubscription.Start"/> is called.
        /// </remarks>
        /// <param name="subject">The subject on which to listen for messages. 
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <returns>An <see cref="IAsyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>.</returns>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is
        /// <c>null</c> or entirely whitespace.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public IAsyncSubscription SubscribeAsync(string subject)
        {
            return subscribeAsync(subject, null, null);
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
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is 
        /// <c>null</c> or entirely whitespace.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public IAsyncSubscription SubscribeAsync(string subject, EventHandler<MsgHandlerEventArgs> handler)
        {
            return subscribeAsync(subject, null, handler);
        }

        /// <summary>
        /// Creates a synchronous queue subscriber on the given <paramref name="subject"/>.
        /// </summary>
        /// <remarks>All subscribers with the same queue name will form the queue group and
        /// only one member of the group will be selected to receive any given message
        /// synchronously.</remarks>
        /// <param name="subject">The subject on which to listen for messages.</param>
        /// <param name="queue">The name of the queue group in which to participate.</param>
        /// <returns>An <see cref="ISyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>, as part of 
        /// the given queue group.</returns>
        public ISyncSubscription SubscribeSync(string subject, string queue)
        {
            return subscribeSync(subject, queue);
        }

        /// <summary>
        /// Creates an asynchronous queue subscriber on the given <paramref name="subject"/>.
        /// </summary>
        /// <remarks>
        /// The <see cref="IAsyncSubscription"/> returned will not start receiving messages until
        /// <see cref="IAsyncSubscription.Start"/> is called.
        /// </remarks>
        /// <param name="subject">The subject on which to listen for messages.
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <param name="queue">The name of the queue group in which to participate.</param>
        /// <returns>An <see cref="IAsyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>.</returns>
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is 
        /// <c>null</c> or entirely whitespace.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public IAsyncSubscription SubscribeAsync(string subject, string queue)
        {
            return subscribeAsync(subject, queue, null);
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
        /// <exception cref="NATSBadSubscriptionException"><paramref name="subject"/> is 
        /// <c>null</c> or entirely whitespace.</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public IAsyncSubscription SubscribeAsync(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler)
        {
            return subscribeAsync(subject, queue, handler);
        }

        // unsubscribe performs the low level unsubscribe to the server.
        // Use Subscription.Unsubscribe()
        internal Task unsubscribe(Subscription sub, int max, bool drain, int timeout)
        {
            var task = CompletedTask.Get();
            
            lock (mu)
            {
                if (isClosed())
                    throw new NATSConnectionClosedException();

                if (!subs.TryGetValue(sub.sid, out var s) || s == null)
                    return task;

                if (max > 0)
                {
                    s.max = max;
                }
                else if (drain == false)
                {
                    removeSub(s);
                }

                if (drain)
                {
                    task = Task.Run(() => checkDrained(s, timeout));
                }

                // We will send all subscriptions when reconnecting
                // so that we can supress here.
                if (!isReconnecting())
                    writeString(IC.unsubProto, s.sid.ToNumericString(), max.ToNumericString());

            }

            kickFlusher();

            return task;
        }

        internal void removeSubSafe(Subscription s)
        {
            lock (mu)
            {
                removeSub(s);
            }
        }

        // caller must lock
        internal virtual void removeSub(Subscription s)
        {
            subs.Remove(s.sid);
            if (s.mch != null)
            {
                if (s.ownsChannel)
                    s.mch.close();

                s.mch = null;
            }

            s.closed = true;
        }

        // FIXME: This is a hack
        // removeFlushEntry is needed when we need to discard queued up responses
        // for our pings as part of a flush call. This happens when we have a flush
        // call outstanding and we call close.
        private bool removeFlushEntry(SingleUseChannel<bool> chan)
        {
            if (!pongs.TryDequeue(out var start))
                return false;

            var c = start;

            while (true)
            {
                if (c == chan)
                {
                    SingleUseChannel<bool>.Return(c);
                    return true;
                }

                pongs.Enqueue(c);

                if (!pongs.TryDequeue(out c))
                    return false;

                if (c == start)
                    break;
            }

            return false;
        }

        // The caller must lock this method.
        private void sendPing(SingleUseChannel<bool> ch)
        {
            if (ch != null)
                pongs.Enqueue(ch);

            bw.Write(PING_P_BYTES, 0, PING_P_BYTES_LEN);
            bw.Flush();
        }

        private void saveFlushException(Exception e)
        {
            if (lastEx != null && !(lastEx is NATSSlowConsumerException))
                lastEx = e;
        }

        /// <summary>
        /// Performs a round trip to the server and returns when it receives the internal reply, or throws
        /// a <see cref="NATSTimeoutException"/> exception if the NATS Server does not reply in time.
        /// </summary>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is less than or equal to zero (<c>0</c>).</exception>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the response.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while executing the
        /// request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public void Flush(int timeout)
        {
            if (timeout <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    "timeout",
                    "Timeout must be greater than 0");
            }

            SingleUseChannel<bool> ch = SingleUseChannel<bool>.GetOrCreate();

            try
            {
                lock (mu)
                {
                    if (isClosed())
                        throw new NATSConnectionClosedException();

                    sendPing(ch);
                }

                bool rv = ch.get(timeout);
                if (!rv)
                {
                    throw new NATSConnectionClosedException();
                }

                SingleUseChannel<bool>.Return(ch);
            }
            catch (Exception e)
            {
                removeFlushEntry(ch);

                // Note, don't call saveFlushException in a finally clause
                // because we don't know if the caller will handle the rethrown 
                // exception.
                if (e is NATSTimeoutException || e is NATSConnectionClosedException)
                {
                    saveFlushException(e);
                    throw;
                }
                else
                {
                    // wrap other system exceptions
                    var ex = new NATSException("Flush error.", e);
                    saveFlushException(ex);
                    throw ex;
                }
            }
        }

        /// <summary>
        /// Performs a round trip to the server and returns when it receives the internal reply.
        /// </summary>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSTimeoutException">A timeout occurred while sending the request or receiving the response.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while executing the
        /// request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public void Flush()
        {
            Flush(DEFAULT_FLUSH_TIMEOUT);
        }

        /// <summary>
        /// Immediately flushes the underlying connection buffer if the connection is valid.
        /// </summary>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while executing the
        /// request. See <see cref="Exception.InnerException"/> for more details.</exception>
        public void FlushBuffer()
        {
            lock (mu)
            {
                if (isClosed())
                    throw new NATSConnectionClosedException();

                if (status == ConnState.CONNECTED)
                    bw.Flush();
            }
        }

        // resendSubscriptions will send our subscription state back to the
        // server. Used in reconnects
        private void resendSubscriptions()
        {
            foreach (Subscription s in subs.Values)
            {
                if (s is IAsyncSubscription)
                    ((AsyncSubscription)s).enableAsyncProcessing();

                writeString(IC.subProto, s.Subject, s.Queue, s.sid.ToNumericString());
            }

            bw.Flush();
        }

        // This will clear any pending flush calls and release pending calls.
        // Lock must be held by the caller.
        private void clearPendingFlushCalls()
        {
            while(pongs.TryDequeue(out var ch))
                ch?.add(true);
        }


        // Clears any in-flight requests by cancelling them all
        // Caller must lock
        private void clearPendingRequestCalls()
        {
            lock (mu)
            {
                foreach (var request in waitingRequests)
                {
                    request.Value.Waiter.TrySetCanceled();
                }
                waitingRequests.Clear();
            }
        }


        // Low level close call that will do correct cleanup and set
        // desired status. Also controls whether user defined callbacks
        // will be triggered. The lock should not be held entering this
        // function. This function will handle the locking manually.
        private void close(ConnState closeState, bool invokeDelegates, Exception error = null)
        {
            lock (mu)
            {
                if (isClosed())
                {
                    status = closeState;
                    return;
                }

                status = ConnState.CLOSED;

                // Kick the routines so they fall out.
                // fch will be closed on finalizer
                kickFlusher();
            }

            lock (mu)
            {
                // Clear any queued pongs, e.g. pending flush calls.
                clearPendingFlushCalls();
                if (pending != null)
                    pending.Dispose();

                // Clear any pending request calls
                clearPendingRequestCalls();

                stopPingTimer();

                // Close sync subscriber channels and release any
                // pending NextMsg() calls.
                foreach (Subscription s in subs.Values)
                {
                    s.close();
                }

                subs.Clear();

                // perform appropriate callback is needed for a
                // disconnect;
                if (invokeDelegates && conn.isSetup())
                {
                    scheduleConnEvent(Opts.DisconnectedEventHandler, error);
                }

                // Go ahead and make sure we have flushed the outbound buffer.
                status = ConnState.CLOSED;
                if (conn.isSetup())
                {
                    if (bw != null)
                    {
                        try
                        {
                            bw.Flush();
                        }
                        catch (Exception) { /* ignore */ }

                        try
                        {
                            bw.Dispose();
                        }
                        catch (Exception)
                        {
                            /* ignore */
                        }
                        finally
                        {
                            bw = null;
                        }
                    }

                    if (br != null)
                    {
                        try
                        {
                            br.Dispose();
                        }
                        catch
                        {
                            // ignored
                        }
                        finally
                        {
                            br = null;
                        }
                    }

                    conn.teardown();
                }

                if (invokeDelegates)
                {
                    scheduleConnEvent(opts.ClosedEventHandler, error);
                }

                status = closeState;
            }
        }

        /// <summary>
        /// Closes the <see cref="Connection"/> and all associated
        /// subscriptions.
        /// </summary>
        /// <seealso cref="IsClosed"/>
        /// <seealso cref="State"/>
        public void Close()
        {
            close(ConnState.CLOSED, true, lastEx);
            callbackScheduler.ScheduleStop();
            disableSubChannelPooling();
        }

        // assume the lock is held.
        private bool isClosed()
        {
            return (status == ConnState.CLOSED);
        }

        /// <summary>
        /// Returns a value indicating whether or not the <see cref="Connection"/>
        /// instance is closed.
        /// </summary>
        /// <returns><c>true</c> if and only if the <see cref="Connection"/> is
        /// closed, otherwise <c>false</c>.</returns>
        /// <seealso cref="Close"/>
        /// <seealso cref="State"/>
        public bool IsClosed()
        {
            lock (mu)
            {
                return isClosed();
            }
        }

        // This allows us to know that whatever we have in the client pending
        // is correct and the server will not send additional information.
        private void checkDrained(Subscription s, int timeout)
        {
            if (isClosed() || s == null)
                return;

            // This allows us to know that whatever we have in the client pending
            // is correct and the server will not send additional information.
            Flush();

            // Once we are here we just wait for Pending to reach 0 or
            // any other state to exit this go routine.
            var sw = Stopwatch.StartNew();
            while (true)
            {
                if (IsClosed())
                    return;

                Connection c;
                bool closed;
                long pMsgs;

                // check our subscription state
                lock (s.mu)
                {
                    c = s.conn;
                    closed = s.closed;
                    pMsgs = s.pMsgs;
                }

                if (c == null || closed || pMsgs == 0)
                {
                    lock (mu)
                    {
                        removeSub(s);
                        return;
                    }
                }

                Thread.Sleep(100);
                if (sw.ElapsedMilliseconds > timeout)
                {
                    var ex = new NATSTimeoutException("Drain timed out.");
                    pushDrainException(s, ex);
                    throw ex;
                }
            }

        }

        // processSlowConsumer will set SlowConsumer state and fire the
        // async error handler if registered.
        internal void pushDrainException(Subscription s, Exception ex)
        {
            EventHandler<ErrEventArgs> aseh = opts.AsyncErrorEventHandler;
            if (aseh != null)
            {
                callbackScheduler.Add(
                    () => { aseh(s, new ErrEventArgs(this, s, ex.Message)); }
                );
            }
        }

        private void drain(int timeout)
        {
            ICollection<Subscription> lsubs = null;
            bool timedOut = false;

            lock (mu)
            {
                if (isClosed())
                    throw new NATSConnectionClosedException();

                // if we're already draining, exit.
                if (isDrainingSubs() || isDrainingPubs())
                    return;

                lsubs = subs.Values;
                status = ConnState.DRAINING_SUBS;
            }

            Task[] tasks = new Task[lsubs.Count];
            int i = 0;
            foreach (var s in lsubs)
            {
                try
                {
                    tasks[i] = s.InternalDrain(timeout);
                    i++;
                }
                catch (Exception)
                {
                    timedOut = true;
                    // Internal drain will push the exception.
                }
            }

            if (Task.WaitAll(tasks, timeout) == false || SubscriptionCount > 0)
            {
                timedOut = true;
                pushDrainException(null, new NATSTimeoutException("Drain timeout."));
            }

            // flip state
            lock (mu)
            {
                status = ConnState.DRAINING_PUBS;
            }

            try
            {
                Flush();
            }
            catch (Exception ex)
            {
                timedOut = true;
                pushDrainException(null, ex);
            }

            // Move to the closed state.
            Close();

            if (timedOut)
                throw new NATSTimeoutException("Drain timeout.");
        }

        /// <summary>
        /// Drains a connection for graceful shutdown.
        /// </summary>
        /// <remarks>
        /// Drain will put a connection into a drain state. All subscriptions will
        /// immediately be put into a drain state. Upon completion, the publishers
        /// will be drained and can not publish any additional messages. Upon draining
        /// of the publishers, the connection will be closed. Use the 
        /// <see cref="Options.ClosedEventHandler"/> option to know when the connection
        /// has moved from draining to closed.
        /// </remarks>
        /// <seealso cref="Close()"/>
        public void Drain()
        {
            Drain(Defaults.DefaultDrainTimeout);
        }

        /// <summary>
        /// Drains a connection for graceful shutdown.
        /// </summary>
        /// <remarks>
        /// Drain will put a connection into a drain state. All subscriptions will
        /// immediately be put into a drain state. Upon completion, the publishers
        /// will be drained and can not publish any additional messages. Upon draining
        /// of the publishers, the connection will be closed. Use the 
        /// <see cref="Options.ClosedEventHandler"/> option to know when the connection
        /// has moved from draining to closed.
        /// </remarks>
        /// <seealso cref="Close()"/>
        /// <param name="timeout">The duration to wait before draining.</param> 
        public void Drain(int timeout)
        {
            if (timeout <= 0)
                throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be greater than zero.");

            drain(timeout);
        }

        /// <summary>
        /// Drains a connection for graceful shutdown.
        /// </summary>
        /// <remarks>
        /// Drain will put a connection into a drain state. All subscriptions will
        /// immediately be put into a drain state. Upon completion, the publishers
        /// will be drained and can not publish any additional messages. Upon draining
        /// of the publishers, the connection will be closed. Use the 
        /// <see cref="Options.ClosedEventHandler"/> option to know when the connection
        /// has moved from draining to closed.
        /// </remarks>
        /// <seealso cref="Close()"/>
        /// <returns>A task that represents the asynchronous drain operation.</returns>
        public Task DrainAsync()
        {
            return DrainAsync(Defaults.DefaultDrainTimeout);
        }

        /// <summary>
        /// Drains a connection for graceful shutdown.
        /// </summary>
        /// <remarks>
        /// Drain will put a connection into a drain state. All subscriptions will
        /// immediately be put into a drain state. Upon completion, the publishers
        /// will be drained and can not publish any additional messages. Upon draining
        /// of the publishers, the connection will be closed. Use the 
        /// <see cref="Options.ClosedEventHandler"/> option to know when the connection
        /// has moved from draining to closed.
        /// </remarks>
        /// <seealso cref="Close()"/>
        /// <param name="timeout">The duration to wait before draining.</param> 
        /// <returns>A task that represents the asynchronous drain operation.</returns>
        public Task DrainAsync(int timeout)
        {
            if (timeout <= 0)
                throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be greater than zero.");

            return Task.Run(() => drain(timeout));
        }

        // assume the lock is held.
        private bool isDrainingPubs()
        {
            return (status == ConnState.DRAINING_PUBS);
        }

        // assume the lock is held.
        private bool isDrainingSubs()
        {
            return (status == ConnState.DRAINING_SUBS);
        }

        /// <summary>
        /// Returns a value indicating whether or not the <see cref="Connection"/>
        /// instance is draining.
        /// </summary>
        /// <returns><c>true</c> if and only if the <see cref="Connection"/> is
        /// draining, otherwise <c>false</c>.</returns>
        /// <seealso cref="IConnection.Drain()"/>
        /// <seealso cref="State"/>
        public bool IsDraining()
        {
            lock (mu)
            {
                return (status == ConnState.DRAINING_SUBS || status == ConnState.DRAINING_PUBS);
            }
        }

        /// <summary>
        /// Returns a value indicating whether or not the <see cref="Connection"/>
        /// is currently reconnecting.
        /// </summary>
        /// <returns><c>true</c> if and only if the <see cref="Connection"/> is
        /// reconnecting, otherwise <c>false</c>.</returns>
        /// <seealso cref="State"/>
        public bool IsReconnecting()
        {
            lock (mu)
            {
                return isReconnecting();
            }
        }

        /// <summary>
        /// Gets the current state of the <see cref="Connection"/>.
        /// </summary>
        /// <seealso cref="ConnState"/>
        public ConnState State
        {
            get
            {
                lock (mu)
                {
                    return status;
                }
            }
        }

        /// <summary>
        /// Get the number of active subscriptions.
        /// </summary>
        public int SubscriptionCount => subs.Count;

        private bool isReconnecting()
        {
            lock (mu)
            {
                return (status == ConnState.RECONNECTING);
            }
        }

        // Test if Conn is connected or connecting.
        private bool isConnected()
        {
            return (status == ConnState.CONNECTING || status == ConnState.CONNECTED);
        }

        /// <summary>
        /// Gets the statistics tracked for the <see cref="Connection"/>.
        /// </summary>
        /// <seealso cref="ResetStats"/>
        public IStatistics Stats
        {
            get
            {
                lock (mu)
                {
                    return new Statistics(this.stats);
                }
            }
        }

        /// <summary>
        /// Resets the associated statistics for the <see cref="Connection"/>.
        /// </summary>
        /// <seealso cref="Stats"/>
        public void ResetStats()
        {
            lock (mu)
            {
                stats.clear();
            }
        }

        /// <summary>
        /// Gets the maximum size in bytes of a payload sent
        /// to the connected NATS Server.
        /// </summary>
        /// <seealso cref="Publish(Msg)"/>
        /// <seealso cref="Publish(string, byte[])"/>
        /// <seealso cref="Publish(string, string, byte[])"/>
        /// <seealso cref="Request(string, byte[])"/>
        /// <seealso cref="Request(string, byte[], int)"/>
        /// <seealso cref="RequestAsync(string, byte[])"/>
        /// <seealso cref="RequestAsync(string, byte[], CancellationToken)"/>
        /// <seealso cref="RequestAsync(string, byte[], int)"/>
        /// <seealso cref="RequestAsync(string, byte[], int, CancellationToken)"/>
        public long MaxPayload
        {
            get
            {
                lock (mu)
                {
                    return info.MaxPayload;
                }
            }
        }

        /// <summary>
        /// Returns a string representation of the
        /// value of this <see cref="Connection"/> instance.
        /// </summary>
        /// <returns>A string that represents the current instance.</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{");
            sb.AppendFormat("url={0};", url);
            sb.AppendFormat("info={0};", info);
            sb.AppendFormat("status={0};", status);
            sb.Append("Subscriptions={");
            foreach (Subscription s in subs.Values)
            {
                sb.Append("Subscription {" + s.ToString() + "};");
            }
            sb.Append("};");

            string[] servers = Servers;
            if (servers != null)
            {
                bool printedFirst = false;
                sb.Append("Servers {");
                foreach (string s in servers)
                {
                    if (printedFirst)
                        sb.Append(",");

                    sb.AppendFormat("[{0}]", s);
                    printedFirst = true;
                }
            }
            sb.Append("}");
            return sb.ToString();
        }

        #region IDisposable Support

        // To detect redundant calls
        private bool disposedValue = false; 

        /// <summary>
        /// Closes the connection and optionally releases the managed resources.
        /// </summary>
        /// <remarks>In derived classes, do not override the <see cref="Close"/> method, instead
        /// put all of the <seealso cref="Connection"/> cleanup logic in your Dispose override.</remarks>
        /// <param name="disposing"><c>true</c> to release both managed
        /// and unmanaged resources; <c>false</c> to release only unmanaged 
        /// resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                disposedValue = true;

                if (disposing)
                {
                    try
                    {
                        Close();
                        callbackScheduler.WaitForCompletion();
                        callbackScheduler.Dispose();
                        conn.Dispose();
                    }
                    catch (Exception)
                    {
                        // No need to throw an exception here
                    }
                }
            }
        }

        /// <summary>
        /// Releases all resources used by the <see cref="Connection"/>.
        /// </summary>
        /// <remarks>This method disposes the connection, by clearing all pending
        /// operations, and closing the connection to release resources.</remarks>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region JetStream

        public IJetStream CreateJetStreamContext(JetStreamOptions options = null)
        {
            return new JetStream.JetStream(this, options);
        }

        public IJetStreamManagement CreateJetStreamManagementContext(JetStreamOptions options = null)
        {
            return new JetStream.JetStreamManagement(this, options);
        }

        #endregion
    } // class Conn
}
