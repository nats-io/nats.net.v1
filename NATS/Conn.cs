// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Sockets;

// disable XM comment warnings
#pragma warning disable 1591

namespace NATS.Client
{
    /// <summary>
    /// State of the connection.
    /// </summary>
    public enum ConnState
    {
        DISCONNECTED = 0,
        CONNECTED,
        CLOSED,
        RECONNECTING,
        CONNECTING
    }

    internal class ServerInfo
    {
        internal string Id;
        internal string Host;
        internal int Port;
        internal string Version;
        internal bool AuthRequired;
        internal bool SslRequired;
        internal Int64 MaxPayload;

        Dictionary<string, string> parameters = new Dictionary<string, string>();

        // A quick and dirty way to convert the server info string.
        // .NET 4.5/4.6 natively supports JSON, but 4.0 does not, and we
        // don't want to require users to to download a seperate json.NET
        // tool for a minimal amount of parsing.
        internal ServerInfo(string jsonString)
        {
            string[] kv_pairs = jsonString.Split(',');
            foreach (string s in kv_pairs)
                addKVPair(s);

            //parameters = kv_pairs.ToDictionary<string, string>(v => v.Split(',')[0], v=>v.Split(',')[1]);

            Id = parameters["server_id"];
            Host = parameters["host"];
            Port = Convert.ToInt32(parameters["port"]);
            Version = parameters["version"];

            AuthRequired = "true".Equals(parameters["auth_required"]);
            SslRequired = "true".Equals(parameters["ssl_required"]);
            MaxPayload = Convert.ToInt64(parameters["max_payload"]);
        }

        private void addKVPair(string kv_pair)
        {
            string key;
            string value;

            kv_pair.Trim();
            string[] parts = kv_pair.Split(':');
            if (parts[0].StartsWith("{"))
                key = parts[0].Substring(1);
            else
                key = parts[0];

            if (parts[1].EndsWith("}"))
                value = parts[1].Substring(0, parts[1].Length - 1);
            else
                value = parts[1];

            key.Trim();
            value.Trim();

            // trim off the quotes.
            key = key.Substring(1, key.Length - 2);

            // bools and numbers may not have quotes.
            if (value.StartsWith("\""))
            {
                value = value.Substring(1, value.Length - 2);
            }

            parameters.Add(key, value);
        }

    }

    /// <summary>
    /// Represents the connection to the server.
    /// </summary>
    public class Connection : IConnection, IDisposable
    {
        Statistics stats = new Statistics();

        // NOTE: We aren't using Mutex here to support enterprises using
        // .NET 4.0.
        readonly object mu = new Object(); 


        private Random r = null;

        Options opts = new Options();

        // returns the options used to create this connection.
        public Options Opts
        {
            get { return opts; }
        }

        List<Task> wg = new List<Task>(2);


        private Uri             url     = null;
        private LinkedList<Srv> srvPool = new LinkedList<Srv>();

        // we have a buffered reader for writing, and reading.
        // This is for both performance, and having to work around
        // interlinked read/writes (supported by the underlying network
        // stream, but not the BufferedStream).
        private BufferedStream  bw      = null;
        private BufferedStream  br      = null;
        private MemoryStream    pending = null;

        Object flusherLock     = new Object();
        bool   flusherKicked = false;
        bool   flusherDone     = false;

        private ServerInfo     info = null;
        private Int64          ssid = 0;

        private ConcurrentDictionary<Int64, Subscription> subs = 
            new ConcurrentDictionary<Int64, Subscription>();
        
        private Queue<Channel<bool>> pongs = new Queue<Channel<bool>>();

        internal MsgArg   msgArgs = new MsgArg();

        internal ConnState status = ConnState.CLOSED;

        Exception lastEx;

        Parser              ps = null;
        System.Timers.Timer ptmr = null;

        int                 pout = 0;

        // Prepare static protocol messages to minimize encoding costs.
        private byte[] pingProtoBytes = null;
        private int    pingProtoBytesLen;
        private byte[] pongProtoBytes = null;
        private int    pongProtoBytesLen;
        private byte[] _CRLF_AS_BYTES = Encoding.UTF8.GetBytes(IC._CRLF_);

        // Use a string builder to generate protocol messages.
        StringBuilder   publishSb    = new StringBuilder(Defaults.scratchSize);

        TCPConnection conn = new TCPConnection();


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
        private class TCPConnection
        {
            /// A note on the use of streams.  .NET provides a BufferedStream
            /// that can sit on top of an IO stream, in this case the network
            /// stream. It increases performance by providing an additional
            /// buffer.
            /// 
            /// So, here's what we have:
            ///     Client code
            ///          ->BufferedStream (bw)
            ///              ->NetworkStream (srvStream)
            ///                  ->TCPClient (srvClient);
            /// 
            /// TODO:  Test various scenarios for efficiency.  Is a 
            /// BufferedReader directly over a network stream really 
            /// more efficient for NATS?
            /// 
            Object        mu     = new Object();
            TcpClient     client = null;
            NetworkStream writeStream = null;
            NetworkStream readStream = null;

            internal void open(Srv s, int timeoutMillis)
            {
                lock (mu)
                {

                    client = new TcpClient(s.url.Host, s.url.Port);
#if async_connect
                    client = new TcpClient();
                    IAsyncResult r = client.BeginConnect(s.url.Host, s.url.Port, null, null);

                    if (r.AsyncWaitHandle.WaitOne(
                        TimeSpan.FromMilliseconds(timeoutMillis)) == false)
                    {
                        client = null;
                        throw new NATSConnectionException("Timeout");
                    }
                    client.EndConnect(r);
#endif

                    client.NoDelay = false;

                    client.ReceiveBufferSize = Defaults.defaultBufSize;
                    client.SendBufferSize    = Defaults.defaultBufSize;

                    writeStream = client.GetStream();
                    readStream = new NetworkStream(client.Client);
                }
            }

            internal int ConnectTimeout
            {
                set
                {
                    ConnectTimeout = value;
                }
            }

            internal int SendTimeout
            {
                set
                {
                    client.SendTimeout = value;
                }
            }

            internal bool isSetup()
            {
                return (client != null);
            }

            internal void teardown()
            {
                lock (mu)
                {
                    TcpClient c = client;
                    NetworkStream ws = writeStream;
                    NetworkStream rs = readStream;

                    client = null;
                    writeStream = null;
                    readStream = null;

                    try
                    {
                        rs.Dispose();
                        ws.Dispose();
                        c.Close();
                    }
                    catch (Exception)
                    {
                        // ignore
                    }
                }
            }

            internal BufferedStream getReadBufferedStream(int size)
            {
                return new BufferedStream(readStream, size);
            }

            internal BufferedStream getWriteBufferedStream(int size)
            {
                return new BufferedStream(writeStream, size);
            }

            internal bool Connected
            {
                get
                {
                    if (client == null)
                        return false;

                    return client.Connected;
                }
            }

            internal bool DataAvailable
            {
                get
                {
                    if (readStream == null)
                        return false;

                    return readStream.DataAvailable;
                }
            }
        }

        private class ConnectInfo
        {
            bool verbose;
            bool pedantic;
            string user;
            string pass;
            bool ssl;
            string name;
            string lang = Defaults.LangString;
            string version = Defaults.Version;

            internal ConnectInfo(bool verbose, bool pedantic, string user, string pass,
                bool secure, string name)
            {
                this.verbose  = verbose;
                this.pedantic = pedantic;
                this.user     = user;
                this.pass     = pass;
                this.ssl      = secure;
                this.name     = name;
            }

            /// <summary>
            /// .NET 4 does not natively support JSON parsing.  When moving to 
            /// support only .NET 4.5 and above use the JSON support provided
            /// by Microsoft. (System.json)
            /// </summary>
            /// <returns>JSON string repesentation of the current object.</returns>
            internal string ToJson()
            {
                StringBuilder sb = new StringBuilder();

                sb.Append("{");

                sb.AppendFormat("\"verbose\":{0},\"pedantic\":{1},",
                    verbose ? "true" : "false",
                    pedantic ? "true" : "false");
                if (user != null)
                {
                    sb.AppendFormat("\"user\":\"{0}\",", user);
                    if (pass != null)
                        sb.AppendFormat("\"pass\":\"{0}\",", pass);
                }

                sb.AppendFormat(
                    "\"ssl_required\":{0},\"name\":\"{1}\",\"lang\":\"{2}\",\"version\":\"{3}\"",
                    ssl ? "true" : "false", name, lang, version);

                sb.Append("}");

                return sb.ToString();
            }
        }

        // Ensure we cannot instanciate a connection this way.
        private Connection() { }

        internal Connection(Options opts)
        {
            this.opts = opts;
            this.pongs = createPongs();
            this.ps = new Parser(this);
            this.pingProtoBytes = System.Text.Encoding.UTF8.GetBytes(IC.pingProto);
            this.pingProtoBytesLen = pingProtoBytes.Length;
            this.pongProtoBytes = System.Text.Encoding.UTF8.GetBytes(IC.pongProto);
            this.pongProtoBytesLen = pongProtoBytes.Length;
        }


        /// Return bool indicating if we have more servers to try to establish
        /// a connection.
        private bool isServerAvailable()
        {
            return (srvPool.Count > 0);
        }

        // Return the currently selected server
        private Srv currentServer
        {
            get
            {
                if (!isServerAvailable())
                    return null;

                foreach (Srv s in srvPool)
                {
                    if (s.url.OriginalString.Equals(this.url.OriginalString))
                        return s;
                }

                return null;
            }
        }

        // Pop the current server and put onto the end of the list. Select head of list as long
        // as number of reconnect attempts under MaxReconnect.
        private Srv selectNextServer()
        {
            Srv s = this.currentServer;
            if (s == null)
                throw new NATSNoServersException("No servers are configured.");

            int num = srvPool.Count;
            int maxReconnect = opts.MaxReconnect;

            // remove the current server.
            srvPool.Remove(s);

            if (maxReconnect > 0 && s.reconnects < maxReconnect)
            {
                // if we haven't surpassed max reconnects, add it
                // to try again.
                srvPool.AddLast(s);
            }

            if (srvPool.Count <= 0)
            {
                this.url = null;
                return null;
            }

            Srv first = srvPool.First();
            this.url = first.url;

            return first;
        }

        // Will assign the correct server to the Conn.Url
        private void pickServer()
        {
            this.url = null;

            if (!isServerAvailable())
                throw new NATSNoServersException("Unable to choose server; no servers available.");

            this.url = srvPool.First().url;
        }

        private List<string> randomizeList(string[] serverArray)
        {
            List<string> randList = new List<string>();
            List<string> origList = new List<string>(serverArray);

            Random r = new Random();

            while (origList.Count > 0)
            {
                int index = r.Next(0, origList.Count);
                randList.Add(origList[index]);
                origList.RemoveAt(index);
            }

            return randList;
        }

        // Create the server pool using the options given.
        // We will place a Url option first, followed by any
        // Server Options. We will randomize the server pool unlesss
        // the NoRandomize flag is set.
        private void setupServerPool()
        {
            List<string> servers;

            if (!string.IsNullOrWhiteSpace(Opts.Url))
                srvPool.AddLast(new Srv(Opts.Url));

            if (Opts.Servers != null)
            {
                if (Opts.NoRandomize)
                    servers = new List<string>(Opts.Servers);
                else
                    servers = randomizeList(Opts.Servers);

                foreach (string s in servers)
                    srvPool.AddLast(new Srv(s));
            }

            // Place default URL if pool is empty.
            if (srvPool.Count == 0)
                srvPool.AddLast(new Srv(Defaults.Url));

            pickServer();
        }

        // createConn will connect to the server and wrap the appropriate
        // bufio structures. It will do the right thing when an existing
        // connection is in place.
        private bool createConn()
        {
            currentServer.updateLastAttempt();

            try
            {
                conn.open(currentServer, opts.Timeout);

                if (pending != null && bw != null)
                {
                    // flush to the pending buffer;
                    bw.Flush();
                }

                bw = conn.getWriteBufferedStream(Defaults.defaultBufSize * 6);
                br = conn.getReadBufferedStream(Defaults.defaultBufSize * 6);
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
            // TODO:  Notes... SSL for beta?  Encapsulate/overide writes to work with SSL and
            // standard streams.  Buffered writer with an SSL stream?
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
                    Task.WaitAll(this.wg.ToArray());
                }
                catch (Exception) { }
            }
        }

        private void spinUpSocketWatchers()
        {
            Task t = null;

            waitForExits();

            t = new Task(() => { readLoop(); });
            t.Start();
            wg.Add(t);

            t = new Task(() => { flusher(); });
            t.Start();
            wg.Add(t);

            lock (mu)
            {
                this.pout = 0;

                if (Opts.PingInterval > 0)
                {
                    if (ptmr == null)
                    {
                        ptmr = new System.Timers.Timer(Opts.PingInterval);
                        ptmr.Elapsed += pingTimerEventHandler;
                        ptmr.AutoReset = true;
                        ptmr.Enabled = true;
                        ptmr.Start();
                    }
                    else
                    {
                        ptmr.Stop();
                        ptmr.Interval = Opts.PingInterval;
                        ptmr.Start();
                    }
                }

            }
        }

        private void pingTimerEventHandler(Object sender, EventArgs args)
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

                sendPing(null);
            }
        }

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
        /// Returns the id of the server currently connected.
        /// </summary>
        public string ConnectedId
        {
            get
            {
                lock (mu)
                {
                    if (status != ConnState.CONNECTED)
                        return IC._EMPTY_;

                    return this.info.Id;
                }
            }
        }

        private Queue<Channel<bool>> createPongs()
        {
            Queue<Channel<bool>> rv = new Queue<Channel<bool>>();
            return rv;
        }

        // Process a connected connection and initialize properly.
        // Caller must lock.
        private void processConnectInit()
        {
            this.status = ConnState.CONNECTING;

            processExpectedInfo();

            sendConnect();

            new Task(() => { spinUpSocketWatchers(); }).Start();
        }

        internal void connect()
        {
            setupServerPool();
            // Create actual socket connection
            // For first connect we walk all servers in the pool and try
            // to connect immediately.
            bool connected = false;
            foreach (Srv s in srvPool)
            {
                this.url = s.url;
                try
                {
                    lastEx = null;
                    lock (mu)
                    {
                        if (createConn())
                        {
                            s.didConnect = true;
                            s.reconnects = 0;

                            processConnectInit();

                            connected = true;
                        }
                    }

                }
                catch (Exception e)
                {
                    lastEx = e;
                    close(ConnState.DISCONNECTED, false);
                    lock (mu)
                    {
                        this.url = null;
                    }
                }

                if (connected)
                    break;

            } // for

            lock (mu)
            {
                if (this.status != ConnState.CONNECTED)
                {
                    if (this.lastEx == null)
                        this.lastEx = new NATSNoServersException("Unable to connect to a server.");

                    throw this.lastEx;
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
            if (Opts.Secure && info.SslRequired)
            {
                throw new NATSSecureConnWantedException();
            }
            else if (info.SslRequired && !Opts.Secure)
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
                return;
            }
            finally
            {
                conn.SendTimeout = 0;
            }

            if (!IC._INFO_OP_.Equals(c.op))
            {
                throw new NATSConnectionException("Protocol exception, INFO not received");
            }

            processInfo(c.args);
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
            String u = url.UserInfo;
            String user = null;
            String pass = null;

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
                    user = u;
                }
            }

            ConnectInfo info = new ConnectInfo(opts.Verbose, opts.Pedantic, user,
                pass, opts.Secure, opts.Name);

            StringBuilder sb = new StringBuilder();

            sb.AppendFormat(IC.conProto, info.ToJson());
            return sb.ToString();
        }


        // caller must lock.
        private void sendConnect()
        {
            try
            {
                writeString(connectProto());
                bw.Write(pingProtoBytes, 0, pingProtoBytesLen);
                bw.Flush();
            }
            catch (Exception ex)
            {
                if (lastEx == null)
                    throw new NATSException("Error sending connect protocol message", ex);
            }

            string result = null;
            try
            {
                StreamReader sr = new StreamReader(br);
                result = sr.ReadLine();

                // Do not close or dispose the stream reader; 
                // we need the underlying BufferedStream.
            }
            catch (Exception ex)
            {
                throw new NATSConnectionException("Connect read error", ex);
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
                        result.TrimStart(IC._ERR_OP_.ToCharArray()));
                }
                else if (result.StartsWith("tls:"))
                {
                    throw new NATSSecureConnRequiredException(result);
                }
                else
                {
                    throw new NATSException(result);
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
            // Do not close or dispose the stream reader - we need the underlying
            // BufferedStream.
            StreamReader sr = new StreamReader(br);
            return new Control(sr.ReadLine());
        }

        private void processDisconnect()
        {
            status = ConnState.DISCONNECTED;
            if (lastEx == null)
                return;

            if (info.SslRequired)
                lastEx = new NATSSecureConnRequiredException();
            else
                lastEx = new NATSConnectionClosedException();
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

                if (ptmr != null)
                {
                    ptmr.Stop();
                }

                if (conn.isSetup())
                {
                    conn.teardown();
                }

                new Task(() => { doReconnect(); }).Start();
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
                bw.Write(pending.GetBuffer(), 0, (int)pending.Length);
                bw.Flush();
            }

            pending = null;
        }

        // Try to reconnect using the option parameters.
        // This function assumes we are allowed to reconnect.
        private void doReconnect()
        {
            // We want to make sure we have the other watchers shutdown properly
            // here before we proceed past this point
            waitForExits();



            // FIXME(dlc) - We have an issue here if we have
            // outstanding flush points (pongs) and they were not
            // sent out, but are still in the pipe.

            // Hold the lock manually and release where needed below.
            Monitor.Enter(mu);

            pending = new MemoryStream();
            bw = new BufferedStream(pending);

            // Clear any errors.
            lastEx = null;

            if (Opts.DisconnectedEventHandler != null)
            {
                Monitor.Exit(mu);

                try
                {
                    Opts.DisconnectedEventHandler(this,
                        new ConnEventArgs(this));
                }
                catch (Exception) { }

                Monitor.Enter(mu);
            }

            Srv s;
            while ((s = selectNextServer()) != null)
            {
                if (lastEx != null)
                    break;

                // Sleep appropriate amount of time before the
                // connection attempt if connecting to same server
                // we just got disconnected from.
                double elapsedMillis = s.TimeSinceLastAttempt.TotalMilliseconds;

                if (elapsedMillis < Opts.ReconnectWait)
                {
                    double sleepTime = Opts.ReconnectWait - elapsedMillis;

                    Monitor.Exit(mu);
                    Thread.Sleep((int)sleepTime);
                    Monitor.Enter(mu);
                }
                else
                {
                    // Yield so other things like unsubscribes can
                    // proceed.
                    Monitor.Exit(mu);
                    Thread.Sleep((int)50);
                    Monitor.Enter(mu);
                }

                if (isClosed())
                    break;

                s.reconnects++;

                try
                {
                    // try to create a new connection
                    createConn();
                }
                catch (Exception)
                {
                    // not yet connected, retry and hold
                    // the lock.
                    continue;
                }

                // We are reconnected.
                stats.reconnects++;

                // Clear out server stats for the server we connected to..
                s.didConnect = true;

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

                s.reconnects = 0;

                // Process CreateConnection logic
                try
                {
                    // Send existing subscription state
                    resendSubscriptions();

                    // Now send off and clear pending buffer
                    flushReconnectPendingItems();

                    // we are connected.
                    status = ConnState.CONNECTED;
                }
                catch (Exception)
                {
                    status = ConnState.RECONNECTING;
                    continue;
                }

                // get the event handler under the lock
                ConnEventHandler reconnectedEh = Opts.ReconnectedEventHandler;

                // Release the lock here, we will return below
                Monitor.Exit(mu);

                // flush everything
                Flush();

                if (reconnectedEh != null)
                {
                    try
                    {
                        reconnectedEh(this, new ConnEventArgs(this));
                    }
                    catch (Exception) { }
                }

                return;

            }

            // we have no more servers left to try.
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
            bool   sb;

            while (true)
            {
                sb = false;
                lock (mu)
                {
                    sb = (isClosed() || isReconnecting());
                    if (sb)
                        this.ps = parser;
                }

                try
                {
                    len = br.Read(buffer, 0, Defaults.defaultReadLength);
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

            lock (mu)
            {
                parser = null;
            }
        }

        // deliverMsgs waits on the delivery channel shared with readLoop and processMsg.
        // It is used to deliver messages to asynchronous subscribers.
        internal void deliverMsgs(Channel<Msg> ch)
        {
            Msg m;

            while (true)
            {
                lock (mu)
                {
                    if (isClosed())
                        return;
                }
 
                m = ch.get(-1);
                if (m == null)
                {
                    // the channel has been closed, exit silently.
                    return;
                }

                // Note, this seems odd message having the sub process itself, 
                // but this is good for performance.
                if (!m.sub.processMsg(m))
                {
                    lock (mu)
                    {
                        removeSub(m.sub);
                    }
                }
            }
        }

        // Roll our own fast conversion - we know it's the right
        // encoding.
        char[] convertToStrBuf = new char[Defaults.scratchSize];

        private string convertToString(byte[] buffer, long length)
        {
            for (int i = 0; i < length; i++)
            {
                convertToStrBuf[i] = (char)buffer[i];
            }

            // This is the copy operation for msg arg strings.
            return new String(convertToStrBuf, 0, (int)length);
        }

        // Here we go ahead and convert the message args into
        // strings, numbers, etc.  The msgArg object is a temporary
        // place to hold them, until we create the message.
        //
        // These strings, once created, are never copied.
        //
        internal void processMsgArgs(byte[] buffer, long length)
        {
            string s = convertToString(buffer, length);
            string[] args = s.Split(' ');

            switch (args.Length)
            {
                case 3:
                    msgArgs.subject = args[0];
                    msgArgs.sid     = Convert.ToInt64(args[1]);
                    msgArgs.reply   = null;
                    msgArgs.size    = Convert.ToInt32(args[2]);
                    break;
                case 4:
                    msgArgs.subject = args[0];
                    msgArgs.sid     = Convert.ToInt64(args[1]);
                    msgArgs.reply   = args[2];
                    msgArgs.size    = Convert.ToInt32(args[3]);
                    break;
                default:
                    throw new NATSException("Unable to parse message arguments: " + s);
            }

            if (msgArgs.size < 0)
            {
                throw new NATSException("Invalid Message - Bad or Missing Size: " + s);
            }
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
                        if (!s.addMessage(new Msg(msgArgs, s, msg, length), opts.subChanLen))
                        {
                            processSlowConsumer(s);
                        }
                    } // maxreached == false

                } // lock s.mu

            } // lock conn.mu

            if (maxReached)
                removeSub(s);
        }

        // processSlowConsumer will set SlowConsumer state and fire the
        // async error handler if registered.
        void processSlowConsumer(Subscription s)
        {
            if (opts.AsyncErrorEventHandler != null && !s.sc)
            {
                new Task(() =>
                {
                    opts.AsyncErrorEventHandler(this,
                        new ErrEventArgs(this, s, "Slow Consumer"));
                }).Start();
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
                        bw.Flush();
                }
            }
        }

        // processPing will send an immediate pong protocol response to the
        // server. The server uses this mechanism to detect dead clients.
        internal void processPing()
        {
            sendProto(pongProtoBytes, pongProtoBytesLen);
        }


        // processPong is used to process responses to the client's ping
        // messages. We use pings for the flush mechanism as well.
        internal void processPong()
        {
            //System.Console.WriteLine("COLIN:  Processing pong.");
            Channel<bool> ch = null;
            lock (mu)
            {
                if (pongs.Count > 0)
                    ch = pongs.Dequeue();

                pout = 0;

                if (ch != null)
                {
                    ch.add(true);
                }
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
        internal void processInfo(string info)
        {
            if (info == null || IC._EMPTY_.Equals(info))
            {
                return;
            }

            this.info = new ServerInfo(info);
        }

        // LastError reports the last error encountered via the Connection.
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

            String s = System.Text.Encoding.UTF8.GetString(
                errorStream.ToArray(), 0, (int)errorStream.Position);

            if (IC.STALE_CONNECTION.Equals(s))
            {
                processOpError(new NATSStaleConnectionException());
            }
            else
            {
                ex = new NATSException(s);
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

        // publish is the internal function to publish messages to a nats-server.
        // Sends a protocol data message by queueing into the bufio writer
        // and kicking the flush go routine. These writes should be protected.
        private void publish(string subject, string reply, byte[] data)
        {
            if (string.IsNullOrWhiteSpace(subject))
            {
                throw new ArgumentException(
                    "Subject cannot be null, empty, or whitespace.",
                    "subject");
            }

            int msgSize = data != null ? data.Length : 0;

            lock (mu)
            {
                // Proactively reject payloads over the threshold set by server.
                if (msgSize > info.MaxPayload)
                    throw new NATSMaxPayloadException();

                if (isClosed())
                    throw new NATSConnectionClosedException();

                if (lastEx != null)
                    throw lastEx;

                // .NET is very performant using string builder.
                publishSb.Clear();

                if (reply == null)
                {
                    publishSb.Append(IC._PUB_P_);
                    publishSb.Append(" ");
                    publishSb.Append(subject);
                    publishSb.Append(" ");
                }
                else
                {
                    publishSb.Append(IC._PUB_P_ + " " + subject + " " +
                        reply + " ");
                }

                publishSb.Append(msgSize);
                publishSb.Append(IC._CRLF_);

                byte[] sendBytes = System.Text.Encoding.UTF8.GetBytes(publishSb.ToString());
                bw.Write(sendBytes, 0, sendBytes.Length);

                if (msgSize > 0)
                {
                    bw.Write(data, 0, msgSize);
                }

                bw.Write(_CRLF_AS_BYTES, 0, _CRLF_AS_BYTES.Length);

                stats.outMsgs++;
                stats.outBytes += msgSize;

                kickFlusher();
            }

        } // publish

        public void Publish(string subject, byte[] data)
        {
            publish(subject, null, data);
        }

        public void Publish(Msg msg)
        {
            publish(msg.Subject, msg.Reply, msg.Data);
        }

        public void Publish(string subject, string reply, byte[] data)
        {
            publish(subject, reply, data);
        }

        private Msg request(string subject, byte[] data, int timeout)
        {
            Msg    m     = null;
            string inbox = NewInbox();

            SyncSubscription s = subscribeSync(inbox, null);
            s.AutoUnsubscribe(1);

            publish(subject, inbox, data);
            m = s.NextMessage(timeout);
            try
            {
                // the auto unsubscribe should handle this.
                s.Unsubscribe();
            }
            catch (Exception) {  /* NOOP */ }

            return m;
        }

        public Msg Request(string subject, byte[] data, int timeout)
        {
            // a timeout of 0 will never succeed - do not allow it.
            if (timeout <= 0)
            {
                throw new ArgumentException(
                    "Timeout must be greater that 0.",
                    "timeout");
            }

            return request(subject, data, timeout);
        }

        public Msg Request(string subject, byte[] data)
        {
            return request(subject, data, -1);
        }

        public string NewInbox()
        {
            if (r == null)
                r = new Random();

            byte[] buf = new byte[13];

            r.NextBytes(buf);

            return IC.inboxPrefix + BitConverter.ToString(buf).Replace("-","");
        }

        internal void sendSubscriptonMessage(AsyncSubscription s)
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

        private AsyncSubscription subscribeAsync(string subject, string queue)
        {
            AsyncSubscription s = null;

            lock (mu)
            {
                if (isClosed())
                    throw new NATSConnectionClosedException();

                s = new AsyncSubscription(this, subject, queue);

                addSubscription(s);
            }

            return s;
        }

        // subscribe is the internal subscribe 
        // function that indicates interest in a subject.
        private SyncSubscription subscribeSync(string subject, string queue)
        {
            SyncSubscription s = null;

            lock (mu)
            {
                if (isClosed())
                    throw new NATSConnectionClosedException();

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

        public ISyncSubscription SubscribeSync(string subject)
        {
            return subscribeSync(subject, null);
        }

        public IAsyncSubscription SubscribeAsync(string subject)
        {
            return subscribeAsync(subject, null);
        }

        public ISyncSubscription SubscribeSync(string subject, string queue)
        {
            return subscribeSync(subject, queue);
        }

        public IAsyncSubscription SubscribeAsync(string subject, string queue)
        {
            return subscribeAsync(subject, queue);
        }

        // unsubscribe performs the low level unsubscribe to the server.
        // Use Subscription.Unsubscribe()
        internal void unsubscribe(Subscription sub, int max)
        {
            lock (mu)
            {
                if (isClosed())
                    throw new NATSConnectionClosedException();

                Subscription s = subs[sub.sid];
                if (s == null)
                {
                    // already unsubscribed
                    return;
                }

                if (max > 0)
                {
                    s.max = max;
                }
                else
                {
                    removeSub(s);
                }

                // We will send all subscriptions when reconnecting
                // so that we can supress here.
                if (!isReconnecting())
                    writeString(IC.unsubProto, s.sid, max);

            }

            kickFlusher();
        }

        internal void removeSub(Subscription s)
        {
            Subscription o;

            subs.TryRemove(s.sid, out o);
            if (s.mch != null)
            {
                s.mch.close();
                s.mch = null;
            }

            s.conn = null;
        }

        // FIXME: This is a hack
        // removeFlushEntry is needed when we need to discard queued up responses
        // for our pings as part of a flush call. This happens when we have a flush
        // call outstanding and we call close.
        private bool removeFlushEntry(Channel<bool> chan)
        {
            if (pongs == null)
                return false;

            if (pongs.Count == 0)
                return false;
            
            Channel<bool> start = pongs.Dequeue();
            Channel<bool> c = start;

            while (true)
            {
                if (c == chan)
                {
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
        private void sendPing(Channel<bool> ch)
        {
            if (ch != null)
                pongs.Enqueue(ch);

            bw.Write(pingProtoBytes, 0, pingProtoBytesLen);
            bw.Flush();
        }

        private void processPingTimer()
        {
            lock (mu)
            {
                if (status != ConnState.CONNECTED)
                    return;

                // Check for violation
                this.pout++;
                if (this.pout <= Opts.MaxPingsOut)
                {
                    sendPing(null);

                    // reset the timer
                    ptmr.Stop();
                    ptmr.Start();

                    return;
                }
            }

            // if we get here, we've encountered an error.  Process
            // this outside of the lock.
            processOpError(new NATSStaleConnectionException());
        }

        public void Flush(int timeout)
        {
            if (timeout <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    "Timeout must be greater than 0",
                    "timeout");
            }

            Channel<bool> ch = new Channel<bool>(1);
            lock (mu)
            {
                if (isClosed())
                    throw new NATSConnectionClosedException();

                sendPing(ch);
            }

            try
            {
                bool rv = ch.get(timeout);
                if (!rv)
                {
                    lastEx = new NATSConnectionClosedException();
                }
            }
            catch (NATSTimeoutException te)
            {
                lastEx = te;
            }
            catch (Exception e)
            {
                lastEx = new NATSException("Flush channel error.", e);
            }

            if (lastEx != null)
            {
                removeFlushEntry(ch);
                throw lastEx;
            }
        }

        /// <summary>
        /// Flush will perform a round trip to the server and return when it
        /// receives the internal reply.
        /// </summary>
        public void Flush()
        {
            // 60 second default.
            Flush(60000);
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


        // Clear pending flush calls and reset
        private void resetPendingFlush()
        {
            lock (mu)
            {
                clearPendingFlushCalls();
                this.pongs = createPongs();
            }
        }

        // This will clear any pending flush calls and release pending calls.
        private void clearPendingFlushCalls()
        {
            lock (mu)
            {
                // Clear any queued pongs, e.g. pending flush calls.
                foreach (Channel<bool> ch in pongs)
                {
                    if (ch != null)
                        ch.add(true);
                }

                pongs.Clear();
            }
        }


        // Low level close call that will do correct cleanup and set
        // desired status. Also controls whether user defined callbacks
        // will be triggered. The lock should not be held entering this
        // function. This function will handle the locking manually.
        private void close(ConnState closeState, bool invokeDelegates)
        {
            ConnEventHandler disconnectedEventHandler = null;
            ConnEventHandler closedEventHandler = null;

            lock (mu)
            {
                if (isClosed())
                {
                    status = closeState;
                    return;
                }

                status = ConnState.CLOSED;
            }

            // Kick the routines so they fall out.
            // fch will be closed on finalizer
            kickFlusher();

            // Clear any queued pongs, e.g. pending flush calls.
            clearPendingFlushCalls();

            lock (mu)
            {
                if (ptmr != null)
                    ptmr.Stop();

                // Close sync subscriber channels and release any
                // pending NextMsg() calls.
                foreach (Subscription s in subs.Values)
                {
                    s.closeChannel();
                }

                subs.Clear();

                // perform appropriate callback is needed for a
                // disconnect;
                if (invokeDelegates && conn.isSetup() &&
                    Opts.DisconnectedEventHandler != null)
                {
                    // TODO:  Mirror go, but this can result in a callback
                    // being invoked out of order
                    disconnectedEventHandler = Opts.DisconnectedEventHandler;
                    new Task(() => { disconnectedEventHandler(this, new ConnEventArgs(this)); }).Start();
                }

                // Go ahead and make sure we have flushed the outbound buffer.
                status = ConnState.CLOSED;
                if (conn.isSetup())
                {
                    if (bw != null)
                        bw.Flush();

                    conn.teardown();
                }

                closedEventHandler = opts.ClosedEventHandler;
            }

            if (invokeDelegates && closedEventHandler != null)
            {
                try
                {
                    closedEventHandler(this, new ConnEventArgs(this));
                }
                catch (Exception) { }
            }

            lock (mu)
            {
                status = closeState;
            }
        }

        public void Close()
        {
            close(ConnState.CLOSED, true);
        }

        // assume the lock is head.
        private bool isClosed()
        {
            return (status == ConnState.CLOSED);
        }

        public bool IsClosed()
        {
            lock (mu)
            {
                return isClosed();
            }
        }

        public bool IsReconnecting()
        {
            lock (mu)
            {
                return isReconnecting();
            }
        }

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

        public void ResetStats()
        {
            lock (mu)
            {
                this.stats.clear();
            }
        }

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
        /// value of this Connection instance.
        /// </summary>
        /// <returns>String value of this instance.</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{");
            sb.AppendFormat("url={0};", url);
            sb.AppendFormat("info={0};", info);
            sb.AppendFormat("status={0}", status);
            sb.Append("Subscriptions={");
            foreach (Subscription s in subs.Values)
            {
                sb.Append("Subscription {" + s.ToString() + "}");
            }
            sb.Append("}}");

            return sb.ToString();
        }

        void IDisposable.Dispose()
        {
            try
            {
                Close();
            }
            catch (Exception)
            {
                // No need to throw an exception here
            }
        }
    } // class Conn


}
