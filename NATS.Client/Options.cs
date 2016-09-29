// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Text;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;
using System.Reflection;

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
        string name = null;
        bool verbose = false;
        bool pedantic = false;
        bool secure = false;
        bool allowReconnect = true;
        int maxReconnect  = Defaults.MaxReconnect;
        int reconnectWait = Defaults.ReconnectWait;
        int pingInterval  = Defaults.PingInterval;
        int timeout       = Defaults.Timeout;

        internal X509Certificate2Collection certificates = null;
 
        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when a connection is closed.
        /// </summary>
        public EventHandler<ConnEventArgs> ClosedEventHandler = null;

        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when a connection has been disconnected from a server.
        /// </summary>
        public EventHandler<ConnEventArgs> DisconnectedEventHandler = null;

        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when a connection has reconnected to a server.
        /// </summary>
        public EventHandler<ConnEventArgs> ReconnectedEventHandler = null;

        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when an error occurs out of band.
        /// </summary>
        public EventHandler<ErrEventArgs> AsyncErrorEventHandler = null;

        internal int maxPingsOut = Defaults.MaxPingOut;

        internal int subChanLen = 65536;
        internal int subscriberDeliveryTaskCount = 0;

        internal string user;
        internal string password;
        internal string token;

        // Options can only be publicly created through 
        // ConnectionFactory.GetDefaultOptions();
        internal Options() { }

        // Copy constructor
        internal Options(Options o)
        {
            allowReconnect = o.allowReconnect;
            AsyncErrorEventHandler = o.AsyncErrorEventHandler;
            ClosedEventHandler = o.ClosedEventHandler;
            DisconnectedEventHandler = o.DisconnectedEventHandler;
            maxPingsOut = o.maxPingsOut;
            maxReconnect = o.maxReconnect;

            if (o.name != null)
            {
                name = new string(o.name.ToCharArray());
            }

            noRandomize = o.noRandomize;
            pedantic = o.pedantic;
            pingInterval = o.pingInterval;
            ReconnectedEventHandler = o.ReconnectedEventHandler;
            reconnectWait = o.reconnectWait;
            secure = o.secure;
            user = o.user;
            password = o.password;
            token = o.token;
            verbose = o.verbose;
            subscriberDeliveryTaskCount = o.subscriberDeliveryTaskCount;
            
            if (o.servers != null)
            {
                servers = new string[o.servers.Length];
                Array.Copy(o.servers, servers, o.servers.Length);
            }

            subChanLen = o.subChanLen;
            timeout = o.timeout;
            TLSRemoteCertificationValidationCallback = o.TLSRemoteCertificationValidationCallback;

            if (o.url != null)
            {
                url = new String(o.url.ToCharArray());
            }

            if (o.certificates != null)
            {
                certificates = new X509Certificate2Collection(o.certificates);
            }
        }

        internal void processUrlString(string url)
        {
            if (url == null)
                return;

            string[] urls = url.Split(',');
            for (int i = 0; i < urls.Length; i++)
            {
                urls[i] = urls[i].Trim();
            }

            servers = urls;
        }

        /// <summary>
        /// Gets or sets the url used to connect to the NATs server.  This may
        /// contain user information.
        /// </summary>
        public string Url
        {
            get { return url; }
            set { url = value; }
        }

        /// <summary>
        /// Gets or Sets the array of servers that the NATs client will connect to.
        /// </summary>
        public string[] Servers
        {
            get { return servers; }
            set { servers = value; }
        }

        /// <summary>
        /// Gets or Sets the randomization of choosing a server to connect to.
        /// </summary>
        public bool NoRandomize
        {
            get { return noRandomize;  }
            set { noRandomize = value;  }
        }

        /// <summary>
        /// Gets or sets the name of this client.
        /// </summary>
        public string Name
        {
            get { return name; }
            set { name = value; }
        }

        /// <summary>
        /// Gets or sets the verbosity of logging.
        /// </summary>
        public bool Verbose
        {
            get { return verbose;  }
            set { verbose = value; }
        }

        /// <summary>
        /// N/A.
        /// </summary>
        public bool Pedantic
        {
            get { return pedantic; }
            set { pedantic = value; }
        }

        /// <summary>
        /// Get or sets the secure property.   Not currently implemented.
        /// </summary>
        public bool Secure
        {
            get { return secure; }
            set { secure = value; }
        }

        /// <summary>
        /// Gets or Sets the allow reconnect flag.  When set to false,
        /// the NATs client will not attempt to reconnect if a connection
        /// has been lost.
        /// </summary>
        public bool AllowReconnect
        {
            get { return allowReconnect; }
            set { allowReconnect = value; }
        }

        /// <summary>
        /// Gets or sets the maxmimum number of times a connection will
        /// attempt to reconnect.
        /// </summary>
        public int MaxReconnect
        {
            get { return maxReconnect; }
            set { maxReconnect = value; }
        }

        /// <summary>
        /// Gets or Sets the amount of time, in milliseconds, the client will 
        /// wait during a reconnection.
        /// </summary>
        public int ReconnectWait
        {
            get { return reconnectWait; }
            set { reconnectWait = value; }
        }

        /// <summary>
        /// Gets or sets the interval pings will be sent to the server.
        /// Take care to coordinate this value with the server's interval.
        /// </summary>
        public int PingInterval
        {
            get { return pingInterval; }
            set { pingInterval = value; }
        }

        /// <summary>
        /// Gets or sets the timeout when flushing a connection.
        /// </summary>
        public int Timeout
        {
            get { return timeout; }
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException(
                        "Timeout must be zero or greater.");
                }

                timeout = value;
            }
        }

        /// <summary>
        /// Gets or sets the maximum number of outstanding pings before
        /// terminating a connection.
        /// </summary>
        public int MaxPingsOut
        {
            get { return maxPingsOut; }
            set { maxPingsOut = value; }
        }

        /// <summary>
        /// Gets or sets the size of the subscriber channel, or number
        /// of messages the subscriber will buffer internally.
        /// </summary>
        public int SubChannelLength
        {
            get { return subChanLen; }
            set { subChanLen = value; }
        }

        /// <summary>
        /// Gets or sets the user name used when connecting to the NATs server
        /// when not included directly in the URLs.
        /// </summary>
        public string User
        {
            get { return user; }
            set { user = value; }
        }

        /// <summary>
        /// Sets the user password used when connecting to the NATs server
        /// when not included directly in the URLs.
        /// </summary>
        public string Password
        {
            set { password = value; }
        }

        /// <summary>
        /// Gets or sets the token used when connecting to the NATs server
        /// when not included directly in the URLs.
        /// </summary>
        public string Token
        {
            get { return token; }
            set { token = value; }
        }

        /// <summary>
        /// Adds a certifcate for use with a secure connection.
        /// </summary>
        /// <param name="fileName">Path to the certificate file to add.</param>
        public void AddCertificate(string fileName)
        {
            X509Certificate2 cert = new X509Certificate2(fileName);
            AddCertificate(cert);
        }

        /// <summary>
        /// Adds a certificate for use with a secure connection.
        /// </summary>
        /// <param name="certificate">Certificate to add.</param>
        public void AddCertificate(X509Certificate2 certificate)
        {
            if (certificates == null)
                certificates = new X509Certificate2Collection();

            certificates.Add(certificate);
        }

        /// <summary>
        /// Overrides the default NATS RemoteCertificationValidationCallback.
        /// </summary>
        /// <remarks>
        /// The default callback simply checks if there were any protocol
        /// errors.  Overriding this callback useful during testing, or accepting self
        /// signed certificates.
        /// </remarks>
        public RemoteCertificateValidationCallback TLSRemoteCertificationValidationCallback;


        /// <summary>
        /// Sets or gets number of long running tasks to deliver messages
        /// to asynchronous subscribers.  The default is 0 indicating each
        /// asynchronous subscriber has its own channel and task created to 
        /// deliver messages.
        /// </summary>
        /// <remarks>
        /// The default where each subscriber has a delivery task is very 
        /// performant, but does not scale well when large numbers of
        /// subscribers are required in an application.  Setting this value
        /// will limit the number of subscriber channels to the specified number
        /// of long running tasks.  These tasks will process messages for ALL
        /// asynchronous subscribers rather than one task for each subscriber.  
        /// Delivery order by subscriber is still guaranteed.  The shared message
        /// processing channels are still each bounded by the SubChannelLength 
        /// option.  Note, slow subscriber errors will flag the last subscriber 
        /// processed in the tasks, which may not actually be the slowest subscriber.
        /// </remarks>
        public int SubscriberDeliveryTaskCount
        {
            get
            {
                return subscriberDeliveryTaskCount;
            }
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException("SubscriberDeliveryTaskCount must be 0 or greater.");
                }
                subscriberDeliveryTaskCount = value;
            }
        }

        private void appendEventHandler(StringBuilder sb, String name, Delegate eh)
        {
            if (eh != null)
                sb.AppendFormat("{0}={1};", name, eh.GetMethodInfo().Name);
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
            sb.AppendFormat("User={0};", User);
            sb.AppendFormat("Token={0};", Token);
            sb.AppendFormat("SubscriberDeliveryTaskCount={0};", SubscriberDeliveryTaskCount);

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

