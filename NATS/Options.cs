// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Text;

namespace NATS.Client
{
    /// <summary>
    /// This class is used to setup all NATs client options.
    /// </summary>
    public sealed class Options
    {
        string url = null;
        string[] servers = null;
        bool noRandomize = false;
        String name = null;
        bool verbose = false;
        bool pedantic = false;
        bool secure = false;
        bool allowReconnect = true;
        int maxReconnect  = Defaults.MaxReconnect;
        int reconnectWait = Defaults.ReconnectWait;
        int pingInterval  = Defaults.PingInterval;
        int timeout       = Defaults.Timeout;

        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when a connection is closed.
        /// </summary>
        public ConnEventHandler  ClosedEventHandler = null;

        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when a connection has been disconnected from a server.
        /// </summary>
        public ConnEventHandler  DisconnectedEventHandler = null;

        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when a connection has reconnected to a server.
        /// </summary>
        public ConnEventHandler  ReconnectedEventHandler = null;

        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when an error occurs out of band.
        /// </summary>
        public ErrorEventHandler AsyncErrorEventHandler = null;

        internal int maxPingsOut = Defaults.MaxPingOut;

        internal int subChanLen = 40000;

        // Options can only be created through ConnectionFactory.GetDefaultOptions();
        internal Options() { }

        /// <summary>
        /// Gets or sets the url used to connect to the NATs server.  This may
        /// contain user information.
        /// </summary>
        public string Url
        {
            get { return this.url; }
            set { this.url = value; }
        }

        /// <summary>
        /// Gets or Sets the array of servers that the NATs client will connect to.
        /// </summary>
        public string[] Servers
        {
            get { return this.servers; }
            set {  this.servers = value; }
        }

        /// <summary>
        /// Gets or Sets the randomization of choosing a server to connect to.
        /// </summary>
        public bool NoRandomize
        {
            get {  return this.noRandomize;  }
            set { this.noRandomize = value;  }
        }

        /// <summary>
        /// Gets or sets the name of this client.
        /// </summary>
        public string Name
        {
            get {  return this.name; }
            set  {  this.name = value; }
        }

        /// <summary>
        /// Gets or sets the verbosity of logging.
        /// </summary>
        public bool Verbose
        {
            get {  return this.verbose;  }
            set  { this.verbose = value; }
        }

        /// <summary>
        /// N/A.
        /// </summary>
        public bool Pedantic
        {
            get { return this.pedantic; }
            set { this.pedantic = value; }
        }

        /// <summary>
        /// Get or sets the secure property.   Not currently implemented.
        /// </summary>
        public bool Secure
        {
            get { return this.secure; }
            set { this.secure = value; }
        }

        /// <summary>
        /// Gets or Sets the allow reconnect flag.  When set to false,
        /// the NATs client will not attempt to reconnect if a connection
        /// has been lost.
        /// </summary>
        public bool AllowReconnect
        {
            get { return this.allowReconnect; }
            set { this.allowReconnect = value; }
        }

        /// <summary>
        /// Gets or sets the maxmimum number of times a connection will
        /// attempt to reconnect.
        /// </summary>
        public int MaxReconnect
        {
            get { return this.maxReconnect; }
            set { this.maxReconnect = value; }
        }

        /// <summary>
        /// Gets or Sets the amount of time, in milliseconds, the client will 
        /// wait during a reconnection.
        /// </summary>
        public int ReconnectWait
        {
            get { return this.reconnectWait; }
            set { this.reconnectWait = value; }
        }

        /// <summary>
        /// Gets or sets the interval pings will be sent to the server.
        /// Take care to coordinate this value with the server's interval.
        /// </summary>
        public int PingInterval
        {
            get { return this.pingInterval; }
            set { this.pingInterval = value; }
        }

        /// <summary>
        /// Gets or sets the timeout when flushing a connection.
        /// </summary>
        public int Timeout
        {
            get { return this.timeout; }
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException(
                        "Timeout must be zero or greater.");
                }

                this.timeout = value;
            }
        }

        /// <summary>
        /// Gets or sets the maximum number of outstanding pings before
        /// terminating a connection.
        /// </summary>
        public int MaxPingsOut
        {
            get { return this.maxPingsOut; }
            set { this.maxPingsOut = value; }
        }

        /// <summary>
        /// Gets or sets the size of the subscriber channel, or number
        /// of messages the subscriber will buffer internally.
        /// </summary>
        public int SubChannelLength
        {
            get { return this.subChanLen; }
            set { this.subChanLen = value; }
        }

        private void appendEventHandler(StringBuilder sb, String name, Delegate eh)
        {
            if (eh != null)
                sb.AppendFormat("{0}={1};", name, eh.Method.Name);
            else
                sb.AppendFormat("{0}=null;", name);
        }

        /// <summary>
        /// Returns a string representation of the
        /// value of this Options instance.
        /// </summary>
        /// <returns>String value of this instance.</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("{");
            sb.AppendFormat("AllowReconnect={0};", allowReconnect);

            appendEventHandler(sb, "AsyncErrorEventHandler", AsyncErrorEventHandler);
            appendEventHandler(sb, "ClosedEventHandler", ClosedEventHandler);
            appendEventHandler(sb, "DisconnectedEventHandler", DisconnectedEventHandler);

            sb.AppendFormat("MaxPingsOut={0};", MaxPingsOut);
            sb.AppendFormat("MaxReconnect={0};", MaxReconnect);
            sb.AppendFormat("Name={0};", Name == null ? Name : "null");
            sb.AppendFormat("NoRandomize={0};", NoRandomize);
            sb.AppendFormat("Pendantic={0};", Pedantic);
            sb.AppendFormat("PingInterval={0};", PingInterval);
            sb.AppendFormat("ReconnectWait={0};", ReconnectWait);
            sb.AppendFormat("Secure={0};", Secure);

            if (Servers == null)
            {
                sb.AppendFormat("Servers=null;");
            }
            else
            {
                sb.Append("Servers={");
                foreach (string s in servers)
                {
                    sb.AppendFormat("[{0}]", s);
                    if (s != servers[servers.Length-1])
                        sb.AppendFormat(",");
                }
                sb.Append("}");
            }
            sb.AppendFormat("SubChannelLength={0};", SubChannelLength);
            sb.AppendFormat("Timeout={0};", Timeout);
            sb.AppendFormat("Pendantic={0}", Pedantic);
            sb.Append("}");

            return sb.ToString();
        }
    }
}

