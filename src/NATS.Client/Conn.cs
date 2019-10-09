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
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Security.Authentication;
using System.Globalization;
using System.Diagnostics;

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
        readonly private object mu = new Object();

        private Random r = null;

        Options opts = new Options();

        /// <summary>
        /// Gets the configuration options for this instance.
        /// </summary>
        public Options Opts
        {
            get { return opts; }
        }

        List<Thread> wg = new List<Thread>(2);

        private Uri             url     = null;
        private ServerPool srvPool = new ServerPool();

        private Dictionary<string, Uri> urls = new Dictionary<string, Uri>();

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

        private ConcurrentDictionary<Int64, Subscription> subs = 
            new ConcurrentDictionary<Int64, Subscription>();
        
        private Queue<SingleUseChannel<bool>> pongs = new Queue<SingleUseChannel<bool>>();

        internal MsgArg   msgArgs = new MsgArg();

        internal ConnState status = ConnState.CLOSED;

        internal Exception lastEx;

        Timer               ptmr = null;

        int                 pout = 0;

        private AsyncSubscription globalRequestSubscription;
        private TaskCompletionSource<object> globalRequestSubReady;
        private string globalRequestInbox;

        // used to map replies to requests from client (should lock)
        private long nextRequestId = 0;

        private Dictionary<string, InFlightRequest> waitingRequests = 
            new Dictionary<string, InFlightRequest>(StringComparer.OrdinalIgnoreCase);

        // Handles in-flight requests when using the new-style request/reply behavior
        private sealed class InFlightRequest : IDisposable
        {
            public InFlightRequest(CancellationToken token, int timeout)
            {
                this.Waiter = new TaskCompletionSource<Msg>();
                if (token != default(CancellationToken))
                {
                    token.Register(() => this.Waiter.TrySetCanceled());

                    if (timeout > 0)
                    {
                        var timeoutToken = new CancellationTokenSource();

                        var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
                            timeoutToken.Token, token);
                        this.Token = linkedTokenSource.Token;

                        this.timeoutTokenRegistration = timeoutToken.Token.Register(
                            () => this.Waiter.TrySetException(new NATSTimeoutException()));
                        timeoutToken.CancelAfter(timeout);
                    }
                    else
                    {
                        this.Token = token;
                    }
                }
                else
                {
                    if (timeout > 0)
                    {
                        var timeoutToken = new CancellationTokenSource();
                        this.Token = timeoutToken.Token;
                        this.timeoutTokenRegistration = timeoutToken.Token.Register(
                            () => this.Waiter.TrySetException(new NATSTimeoutException()));
                        timeoutToken.CancelAfter(timeout);
                    }
                }
            }

            public string Id { get; set; }
            public CancellationToken Token { get; private set; }
            public TaskCompletionSource<Msg> Waiter { get; private set; }

            private CancellationTokenRegistration tokenRegistration;
            private CancellationTokenRegistration timeoutTokenRegistration;

            public void Register(Action action)
            {
                tokenRegistration = Token.Register(action);
            }

            public void Dispose()
            {
                this.timeoutTokenRegistration.Dispose();
                this.tokenRegistration.Dispose();
            }
        }

        // Prepare protocol messages for efficiency
        private byte[] PING_P_BYTES = null;
        private int    PING_P_BYTES_LEN;

        private byte[] PONG_P_BYTES = null;
        private int    PONG_P_BYTES_LEN;

        private byte[] PUB_P_BYTES = null;
        private int    PUB_P_BYTES_LEN = 0;

        private byte[] CRLF_BYTES = null;
        private int    CRLF_BYTES_LEN = 0;

        byte[] pubProtoBuf = null;

        static readonly int REQ_CANCEL_IVL = 100;

        // 60 second default flush timeout
        static readonly int DEFAULT_FLUSH_TIMEOUT = 60000;

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
#if NET45
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

                    client = new TcpClient();
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
                if (c != null)
                {
#if NET45
                    c.Close();
#else
                    c.Dispose();
#endif
                }
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
                        close(client);

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
#if NET45
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

        // Ensure we cannot instanciate a connection this way.
        private Connection() { }

        /// <summary>
        /// Initializes a new instance of the <see cref="Connection"/> class
        /// with the specified <see cref="Options"/>.
        /// </summary>
        /// <param name="options">The configuration options to use for this 
        /// <see cref="Connection"/>.</param>
        internal Connection(Options options)
        {
            opts = new Options(options);
            pongs = createPongs();

            PING_P_BYTES = Encoding.UTF8.GetBytes(IC.pingProto);
            PING_P_BYTES_LEN = PING_P_BYTES.Length;

            PONG_P_BYTES = Encoding.UTF8.GetBytes(IC.pongProto);
            PONG_P_BYTES_LEN = PONG_P_BYTES.Length;

            PUB_P_BYTES = Encoding.UTF8.GetBytes(IC._PUB_P_);
            PUB_P_BYTES_LEN = PUB_P_BYTES.Length;

            CRLF_BYTES = Encoding.UTF8.GetBytes(IC._CRLF_);
            CRLF_BYTES_LEN = CRLF_BYTES.Length;

            // predefine the start of the publish protocol message.
            buildPublishProtocolBuffer(Defaults.scratchSize);

            callbackScheduler.Start();
        }

        private void buildPublishProtocolBuffer(int size)
        {
            pubProtoBuf = new byte[size];
            Buffer.BlockCopy(PUB_P_BYTES, 0, pubProtoBuf, 0, PUB_P_BYTES_LEN);
        }

        // Ensures that pubProtoBuf is appropriately sized for the given
        // subject and reply.
        // Caller must lock.
        private void ensurePublishProtocolBuffer(string subject, string reply)
        {
            // Publish protocol buffer sizing:
            //
            // PUB_P_BYTES_LEN (includes trailing space)
            //  + SUBJECT field length
            //  + SIZE field maximum + 1 (= log(2147483647) + 1 = 11)
            //  + (optional) REPLY field length + 1

            int pubProtoBufSize = PUB_P_BYTES_LEN
                                + (1 + subject.Length)
                                + (11)
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

                buildPublishProtocolBuffer(pubProtoBufSize);
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
        private bool createConn(Srv s)
        {
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
            catch (Exception)
            {
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
            kickFlusher();

            if (wg.Count > 0)
            {
                try
                {
                    foreach (Thread t in wg)
                    {
                        t.Join();
                    }
                }
                catch (Exception) { }
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

                pout++;

                if (pout > Opts.MaxPingsOut)
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
            pout = 0;

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
            t.Start();
            t.Name = generateThreadName("Reader");
            wg.Add(t);

            ManualResetEvent flusherStartEvent = new ManualResetEvent(false);
            t = new Thread(() => {
                flusherStartEvent.Set();
                flusher();
            });
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

                    return this.info.serverId;
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

        private Queue<SingleUseChannel<bool>> createPongs()
        {
            return new Queue<SingleUseChannel<bool>>();
        }

        // Process a connected connection and initialize properly.
        // Caller must lock.
        private void processConnectInit()
        {
            this.status = ConnState.CONNECTING;

            processExpectedInfo();
            sendConnect();

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
                lock (mu)
                {
                    if (createConn(s))
                    {
                        processConnectInit();
                        exToThrow = null;
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                exToThrow = e;
                close(ConnState.DISCONNECTED, false);
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
                    if (exToThrow == null)
                        exToThrow = new NATSNoServersException("Unable to connect to a server.");

                    throw exToThrow;
                }
            }
        }

        // This will check to see if the connection should be
        // secure. This can be dictated from either end and should
        // only be called after the INIT protocol has been received.
        private void checkForSecure()
        {
            // Check to see if we need to engage TLS
            // Check for mismatch in setups
            if (Opts.Secure && !info.tlsRequired)
            {
                throw new NATSSecureConnWantedException();
            }
            else if (info.tlsRequired && !Opts.Secure)
            {
                throw new NATSSecureConnRequiredException();
            }

            // Need to rewrap with bufio
            if (Opts.Secure)
            {
                makeTLSConn();
            }
        }

        // processExpectedInfo will look for the expected first INFO message
        // sent when a connection is established. The lock should be held entering.
        // Caller must lock.
        private void processExpectedInfo()
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
            checkForSecure();
        }

        private void writeString(string format, object a, object b)
        {
            writeString(String.Format(format, a, b));
        }

        private void writeString(string format, object a, object b, object c)
        {
            writeString(String.Format(format, a, b, c));
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

                var args = new UserSignatureEventArgs(Encoding.ASCII.GetBytes(this.info.nonce));
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

            StringBuilder sb = new StringBuilder();

            sb.AppendFormat(IC.conProto, info.ToJson());

            if (opts.NoEcho && info.protocol < 1)
                throw new NATSProtocolException("Echo is not supported by the server.");

            return sb.ToString();
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
                // we need the underlying stream, so leave it open.
                sr = new StreamReader(br, Encoding.UTF8, false, 512, true);
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
                        result.Substring(IC._ERR_OP_.Length));
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
            using (StreamReader sr = new StreamReader(br, Encoding.ASCII, false, 1024, true))
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

                Thread t = new Thread(() =>
                {
                    doReconnect();
                });
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
#if NET45
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
        private void scheduleConnEvent(EventHandler<ConnEventArgs> connEvent)
        {
            // Schedule a reference to the event handler.
            EventHandler<ConnEventArgs> eh = connEvent;
            if (eh != null)
            {
                callbackScheduler.Add(
                    () => { eh(this, new ConnEventArgs(this)); }
                );
            }
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
            Monitor.Enter(mu);

            // clear any queued pongs, e..g. pending flush calls.
            clearPendingFlushCalls();

            pending = new MemoryStream();
            bw = new BufferedStream(pending);

            // Clear any errors.
            lastEx = null;

            scheduleConnEvent(Opts.DisconnectedEventHandler);

            // TODO:  Look at using a predicate delegate in the server pool to
            // pass a method to, but locking is complex and would need to be
            // reworked.
            Srv cur;
            while ((cur = srvPool.SelectNextServer(Opts.MaxReconnect)) != null)
            {
                url = cur.url;

                lastEx = null;

                // Sleep appropriate amount of time before the
                // connection attempt if connecting to same server
                // we just got disconnected from.
                double elapsedMillis = cur.TimeSinceLastAttempt.TotalMilliseconds;
                double sleepTime = 0;

                if (elapsedMillis < Opts.ReconnectWait)
                {
                    sleepTime = Opts.ReconnectWait - elapsedMillis;
                }

                if (sleepTime <= 0)
                {
                    // Release to allow parallel processes to close,
                    // unsub, etc.  Note:  Use the sleep API - yield is
                    // heavy handed here.
                    sleepTime = 50;
                }

                Monitor.Exit(mu);
                sleep((int)sleepTime);
                Monitor.Enter(mu);

                if (isClosed())
                    break;

                cur.reconnects++;

                try
                {
                    // try to create a new connection
                    createConn(cur);
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
                    processConnectInit();
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
                Monitor.Exit(mu);

                // Make sure to flush everything
                // We have a corner case where the server we just
                // connected to has failed as well - let the reader
                // thread detect this and spawn another reconnect 
                // thread to simplify locking.
                try
                {
                    Flush();
                }
                catch (Exception) { }

                return;
            }

            // Call into close.. we have no more servers left..
            if (lastEx == null)
                lastEx = new NATSNoServersException("Unable to reconnect");

            Monitor.Exit(mu);
            Close();
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
            Parser parser = new Parser(this);
            int    len;

            while (true)
            {
                try
                {
                    len = br.Read(buffer, 0, Defaults.defaultReadLength);

                    // Socket has been closed gracefully by host
                    if (len == 0)
                    {
                        break;
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
                        removeSub(m.sub);
                    }
                }
            }
        }

        // Roll our own fast conversion - we know it's the right
        // encoding. 
        char[] convertToStrBuf = new char[Defaults.scratchSize];

        // Caller must ensure thread safety.
        private string convertToString(byte[] buffer, long length)
        {
            // expand if necessary
            if (length > convertToStrBuf.Length)
            {
                convertToStrBuf = new char[length];
            }

            for (int i = 0; i < length; i++)
            {
                convertToStrBuf[i] = (char)buffer[i];
            }

            // This is the copy operation for msg arg strings.
            return new string(convertToStrBuf, 0, (int)length);
        }

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
        private int[] argEnds = new int[4];
        private int setMsgArgsAryOffsets(byte[] buffer, long length)
        {
            if (convertToStrBuf.Length < length)
            {
                convertToStrBuf = new char[length];
            }

            int count = 0;
            int i = 0;

            // We only support 4 elements in this protocol version
            for ( ; i < length && count < 4; i++)
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
        internal void processMsgArgs(byte[] buffer, long length)
        {
            int argCount = setMsgArgsAryOffsets(buffer, length);

            switch (argCount)
            {
                case 3:
                    msgArgs.subject = new string(convertToStrBuf, 0, argEnds[0]);
                    msgArgs.sid     = ToInt64(buffer, argEnds[0] + 1, argEnds[1]);
                    msgArgs.reply   = null;
                    msgArgs.size    = (int)ToInt64(buffer, argEnds[1] + 1, argEnds[2]);
                    break;
                case 4:
                    msgArgs.subject = new string(convertToStrBuf, 0, argEnds[0]);
                    msgArgs.sid     = ToInt64(buffer, argEnds[0] + 1, argEnds[1]);
                    msgArgs.reply   = new string(convertToStrBuf, argEnds[1] + 1, argEnds[2] - argEnds[1] - 1);
                    msgArgs.size    = (int)ToInt64(buffer, argEnds[2] + 1, argEnds[3]);
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
        internal void processMsg(byte[] msg, long length)
        {
            bool maxReached = false;
            Subscription s;

            lock (mu)
            {
                stats.inMsgs++;
                stats.inBytes += length;

                // In regular message processing, the key should be present,
                // so optimize by using an an exception to handle a missing key.
                // (as opposed to checking with Contains or TryGetValue)
                try
                {
                    s = subs[msgArgs.sid];
                }
                catch (Exception)
                {
                    // this can happen when a subscriber is unsubscribing.
                    return;
                }

                lock (s.mu)
                {
                    maxReached = s.tallyMessage(length);
                    if (maxReached == false)
                    {
                        s.addMessage(new Msg(msgArgs, s, msg, length), opts.subChanLen);
                    } // maxreached == false

                } // lock s.mu

            } // lock conn.mu

            if (maxReached)
                removeSub(s);
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

                // Yield for a millisecond.  This reduces resource contention,
                // increasing throughput by about 50%.
                Thread.Sleep(1);

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
            SingleUseChannel<bool> ch = null;
            lock (mu)
            {
                if (pongs.Count > 0)
                    ch = pongs.Dequeue();

                pout = 0;
            }

            if (ch != null)
            {
                ch.add(true);
            }
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
        internal void processInfo(string json, bool notifyOnServerAddition)
        {
            if (json == null || IC._EMPTY_.Equals(json))
            {
                return;
            }

            info = ServerInfo.CreateFromJson(json);
            var discoveredUrls = info.connectURLs;
 
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
                if (notifyOnServerAddition && serverAdded)
                {
                    scheduleConnEvent(opts.ServerDiscoveredEventHandler);
                }
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

                close(ConnState.CLOSED, invokeDelegates);
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
        private int writePublishProto(byte[] dst, string subject, string reply, int msgSize)
        {
            // skip past the predefined "PUB "
            int index = PUB_P_BYTES_LEN;

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

            return index;
        }

        // publish is the internal function to publish messages to a nats-server.
        // Sends a protocol data message by queueing into the bufio writer
        // and kicking the flush go routine. These writes should be protected.
        internal void publish(string subject, string reply, byte[] data, int offset, int count)
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
                if (count > info.maxPayload)
                    throw new NATSMaxPayloadException();

                if (lastEx != null)
                    throw lastEx;

                ensurePublishProtocolBuffer(subject, reply);

                // write our pubProtoBuf buffer to the buffered writer.
                int pubProtoLen = writePublishProto(pubProtoBuf, subject, reply, count);

                bw.Write(pubProtoBuf, 0, pubProtoLen);

                if (count > 0)
                {
                    bw.Write(data, offset, count);
                }

                bw.Write(CRLF_BYTES, 0, CRLF_BYTES_LEN);

                stats.outMsgs++;
                stats.outBytes += count;

                kickFlusher();
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
            publish(subject, null, data, 0, count);
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
            publish(subject, null, data, offset, count);
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
            publish(msg.Subject, msg.Reply, msg.Data, 0, count);
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
            publish(subject, reply, data, 0, count);
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
            publish(subject, reply, data, offset, count);
        }

        internal virtual Msg request(string subject, byte[] data, int offset, int count, int timeout)
        {
            Msg result = null;

            if (string.IsNullOrWhiteSpace(subject))
            {
                throw new NATSBadSubscriptionException();
            }
            else if (timeout == 0)
            {
                // a timeout of 0 will never succeed - do not allow it.
                throw new ArgumentException("Timeout must not be 0.", "timeout");
            }
            // offset/count checking covered by publish

            if (!opts.UseOldRequestStyle)
            {
                var request = requestSync(subject, data, offset, count, timeout, CancellationToken.None);

                try
                {
                    Flush(timeout > 0 ? timeout : DEFAULT_FLUSH_TIMEOUT);
                    request.Waiter.Task.Wait(timeout);
                    result = request.Waiter.Task.Result;
                }
                catch (AggregateException ae)
                {
                    foreach (var e in ae.Flatten().InnerExceptions)
                    {
                        // we *should* only have one, and it should be
                        // a NATS timeout exception.
                        throw e;
                    }
                }
                catch
                {
                    // Could be a timeout or exception from the flush.
                    throw;
                }
                finally
                {
                    removeOutstandingRequest(request.Id);
                }

                return result;
            }
            else
            {
                return oldRequest(subject, data, offset, count, timeout);
            }
        }

        private InFlightRequest setupRequest(int timeout, CancellationToken token)
        {
            InFlightRequest request = new InFlightRequest(token, timeout);
            // avoid raising TaskScheduler.UnobservedTaskException if the timeout occurs first
            request.Waiter.Task.ContinueWith(t => GC.KeepAlive(t.Exception), TaskContinuationOptions.OnlyOnFaulted);
            bool createSub = false;
            lock (mu)
            {
                if (globalRequestSubReady == null)
                {
                    globalRequestSubReady = new TaskCompletionSource<object>();

                    globalRequestInbox = NewInbox();

                    createSub = true;
                }

                if (nextRequestId < 0)
                {
                    nextRequestId = 0;
                }

                request.Id = (nextRequestId++).ToString(CultureInfo.InvariantCulture);
                request.Register(() => removeOutstandingRequest(request.Id));

                waitingRequests.Add(
                    request.Id,
                    request);
            }

            if (createSub)
            {
                globalRequestSubscription = subscribeAsync(globalRequestInbox + ".*", null, requestResponseHandler);

                globalRequestSubReady.TrySetResult(null);
            }

            return request;
        }

        private InFlightRequest requestSync(string subject, byte[] data, int offset, int count, int timeout, CancellationToken token)
        {
            InFlightRequest request = setupRequest(timeout, token);

            request.Token.ThrowIfCancellationRequested();

            try
            {
                if (globalRequestSubscription == null)
                    globalRequestSubReady.Task.Wait(timeout, request.Token);

                publish(subject, globalRequestInbox + "." + request.Id, data, offset, count);
            }
            catch
            {
                removeOutstandingRequest(request.Id);
                throw;
            }

            return request;
        }

        private Task<Msg> requestAsync(string subject, byte[] data, int offset, int count, int timeout, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(subject))
            {
                throw new NATSBadSubscriptionException();
            }
            else if (timeout == 0)
            {
                // a timeout of 0 will never succeed - do not allow it.
                throw new ArgumentException("Timeout must not be 0.", "timeout");
            }
            // offset/count checking covered by publish

            if (!opts.UseOldRequestStyle)
            {
                return Task.Run(
                    async () =>
                    {
                        InFlightRequest request = setupRequest(timeout, token);

                        request.Token.ThrowIfCancellationRequested();

                        try
                        {
                            if (globalRequestSubscription == null)
                                await globalRequestSubReady.Task;

                            publish(subject, globalRequestInbox + "." + request.Id, data, offset, count);

                            Flush(timeout > 0 ? timeout : DEFAULT_FLUSH_TIMEOUT);
                        }
                        catch
                        {
                            removeOutstandingRequest(request.Id);
                            throw;
                        }

                    // InFlightRequest links the token cancellation
                    return await request.Waiter.Task;
                    },
                    token);
            }
            else
            {
                return oldRequestAsync(subject, data, offset, count, timeout, token);
            }
        }

        private void requestResponseHandler(object sender, MsgHandlerEventArgs e)
        {
            //               \
            //               \/
            //  _INBOX.<nuid>.<requestId>
            string requestId = e.Message.Subject.Substring(globalRequestInbox.Length + 1);
            if (e.Message == null)
            {
                return;
            }

            bool isClosed;
            InFlightRequest request;
            lock (mu)
            {
                isClosed = this.isClosed();

                if (!waitingRequests.TryGetValue(requestId, out request))
                {
                    return;
                }

                waitingRequests.Remove(requestId);
            }

            if (!isClosed)
            {
                request.Waiter.SetResult(e.Message);
            }
            else
            {
                request.Waiter.SetCanceled();
            }
            request.Dispose();
        }

        private void removeOutstandingRequest(string requestId)
        {
            lock (mu)
            {
                // this is fine even if requestId does not exist
                waitingRequests.Remove(requestId);
            }
        }

        private Msg oldRequest(string subject, byte[] data, int offset, int count, int timeout)
        {
            Msg    m     = null;
            string inbox = NewInbox();

            SyncSubscription s = subscribeSync(inbox, null);
            s.AutoUnsubscribe(1);

            publish(subject, inbox, data, offset, count);
            Flush(timeout > 0 ? timeout : DEFAULT_FLUSH_TIMEOUT);
            m = s.NextMessage(timeout);
            s.unsubscribe(false);

            return m;
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
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call
        /// while executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Msg Request(string subject, byte[] data, int timeout)
        {
            int count = data != null ? data.Length : 0;
            return request(subject, data, 0, count, timeout);
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
            return request(subject, data, offset, count, timeout);
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
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Msg Request(string subject, byte[] data)
        {
            int count = data != null ? data.Length : 0;
            return request(subject, data, 0, count, Timeout.Infinite);
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
        /// <param name="offset">The zero-based byte offset in <paramref name="data"/> at which to begin publishing
        /// bytes to the subject.</param>
        /// <param name="count">The number of bytes to be published to the subject.</param>
        /// <returns>A <see cref="Msg"/> with the response from the NATS server.</returns>
        public Msg Request(string subject, byte[] data, int offset, int count)
        {
            return request(subject, data, offset, count, Timeout.Infinite);
        }

        internal virtual Task<Msg> oldRequestAsync(string subject, byte[] data, int offset, int count, int timeout, CancellationToken ct)
        {
            // Simple case without a cancellation token.
            if (ct == null)
            {
                return Task.Run(() => oldRequest(subject, data, offset, count, timeout));
            }

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

                publish(subject, inbox, data, offset, count);
                Flush(timeout > 0 ? timeout: DEFAULT_FLUSH_TIMEOUT);

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
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call
        /// while executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before
        /// it could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(string subject, byte[] data, int timeout)
        {
            int count = data != null ? data.Length : 0;
            return requestAsync(subject, data, 0, count, timeout, CancellationToken.None);
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
            return requestAsync(subject, data, offset, count, timeout, CancellationToken.None);
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
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before
        /// it could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(string subject, byte[] data)
        {
            int count = data != null ? data.Length : 0;
            return requestAsync(subject, data, 0, count, Timeout.Infinite, CancellationToken.None);
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
            return requestAsync(subject, data, offset, count, Timeout.Infinite, CancellationToken.None);
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
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before
        /// it could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(string subject, byte[] data, int timeout, CancellationToken token)
        {
            int count = data != null ? data.Length : 0;
            return requestAsync(subject, data, 0, count, timeout, token);
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
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while 
        /// executing the request. See <see cref="Exception.InnerException"/> for more details.</exception>
        /// <exception cref="OperationCanceledException">The asynchronous operation was cancelled or timed out before it
        /// could be completed.</exception>
        /// <exception cref="IOException">There was a failure while writing to the network.</exception>
        public Task<Msg> RequestAsync(string subject, byte[] data, CancellationToken token)
        {
            int count = data != null ? data.Length : 0;
            return requestAsync(subject, data, 0, count, Timeout.Infinite, token);
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
            return requestAsync(subject, data, offset, count, Timeout.Infinite, token);
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
            if (!opts.UseOldRequestStyle)
            {
                return IC.inboxPrefix + Guid.NewGuid().ToString("N");
            }
            else
            {
                if (r == null)
                    r = new Random(Guid.NewGuid().GetHashCode());

                byte[] buf = new byte[13];

                r.NextBytes(buf);

                return IC.inboxPrefix + BitConverter.ToString(buf).Replace("-","");
            }
        }

        internal void sendSubscriptionMessage(AsyncSubscription s)
        {
            lock (mu)
            {
                // We will send these for all subs when we reconnect
                // so that we can suppress here.
                if (!isReconnecting())
                {
                    writeString(IC.subProto, s.Subject, s.Queue, s.sid);
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

        internal AsyncSubscription subscribeAsync(string subject, string queue,
            EventHandler<MsgHandlerEventArgs> handler)
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

                s = new AsyncSubscription(this, subject, queue);

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
        private SyncSubscription subscribeSync(string subject, string queue)
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

                s = new SyncSubscription(this, subject, queue);

                addSubscription(s);

                // We will send these for all subs when we reconnect
                // so that we can suppress here.
                if (!isReconnecting())
                {
                    writeString(IC.subProto, subject, queue, s.sid);
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
            Task task = null;
            lock (mu)
            {
                if (isClosed())
                    throw new NATSConnectionClosedException();

                Subscription s;
                if (!subs.TryGetValue(sub.sid, out s)
                    || s == null)
                {
                    // already unsubscribed
                    return null;
                }

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
                    writeString(IC.unsubProto, s.sid, max);

            }

            kickFlusher();

            return task;
        }

        internal virtual void removeSub(Subscription s)
        {
            Subscription o;

            subs.TryRemove(s.sid, out o);
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
            if (pongs == null)
                return false;

            if (pongs.Count == 0)
                return false;

            SingleUseChannel<bool> start = pongs.Dequeue();
            SingleUseChannel<bool> c = start;

            while (true)
            {
                if (c == chan)
                {
                    SingleUseChannel<bool>.Return(c);
                    return true;
                }
                else
                {
                    pongs.Enqueue(c);
                }

                c = pongs.Dequeue();

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

        // resendSubscriptions will send our subscription state back to the
        // server. Used in reconnects
        private void resendSubscriptions()
        {
            foreach (Subscription s in subs.Values)
            {
                if (s is IAsyncSubscription)
                    ((AsyncSubscription)s).enableAsyncProcessing();

                writeString(IC.subProto, s.Subject, s.Queue, s.sid);
            }

            bw.Flush();
        }

        // This will clear any pending flush calls and release pending calls.
        // Lock must be held by the caller.
        private void clearPendingFlushCalls()
        {

            // Clear any queued pongs, e.g. pending flush calls.
            foreach (SingleUseChannel<bool> ch in pongs)
            {
                if (ch != null)
                    ch.add(true);
            }
            pongs.Clear();
        }


        // Clears any in-flight requests by cancelling them all
        // Caller must lock
        private void clearPendingRequestCalls()
        {
            foreach (var request in waitingRequests)
            {
                request.Value.Waiter.TrySetCanceled();
            }

            waitingRequests.Clear();
        }


        // Low level close call that will do correct cleanup and set
        // desired status. Also controls whether user defined callbacks
        // will be triggered. The lock should not be held entering this
        // function. This function will handle the locking manually.
        private void close(ConnState closeState, bool invokeDelegates)
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
                    scheduleConnEvent(Opts.DisconnectedEventHandler);
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
                        catch (Exception) { /* ignore */ }
                    }

                    conn.teardown();
                }

                if (invokeDelegates)
                {
                    scheduleConnEvent(opts.ClosedEventHandler);
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
            close(ConnState.CLOSED, true);
            callbackScheduler.ScheduleStop();
            disableSubChannelPooling();
        }

        // assume the lock is head.
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

            lock (mu)
            {
                status = ConnState.DRAINING_SUBS;
            }

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

            lock (mu)
            {
                status = ConnState.DRAINING_SUBS;
            }

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
                    return info.maxPayload;
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
    } // class Conn
}
