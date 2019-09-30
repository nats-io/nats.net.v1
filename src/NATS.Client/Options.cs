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
        bool useOldRequestStyle = false;
        bool secure = false;
        bool allowReconnect = true;
        bool noEcho = false;
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
        /// whenever a new server has joined the cluster.
        /// </summary>
        public EventHandler<ConnEventArgs> ServerDiscoveredEventHandler = null;

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

        /// <summary>
        /// Represents the optional method that is used to fetch and
        /// return the account signed JWT for this user.  Exceptions thrown
        /// here will be passed up to the caller when creating a connection.
        /// </summary>
        internal EventHandler<UserJWTEventArgs> UserJWTEventHandler = null;

        /// <summary>
        /// Represents the optional method that is used to sign a nonce
        /// from the server while authenticating with nkeys. The user
        /// should sign the nonce and set the base64 encoded signature.
        /// Exceptions thrown here will be passed up to the caller when 
        /// creating a connection.
        /// </summary>
        internal EventHandler<UserSignatureEventArgs> UserSignatureEventHandler = null;

        /// <summary>
        /// Sets user credentials using the NATS 2.0 security scheme.
        /// </summary>
        /// <param name="credentialsPath">A user JWT, e.g user.jwt</param>
        /// <param name="privateKeyPath">Private Key file</param>
        public void SetUserCredentials(string credentialsPath, string privateKeyPath)
        {
            if (string.IsNullOrWhiteSpace(credentialsPath))
                throw new ArgumentException("Invalid credentials path", nameof(credentialsPath));
            if (string.IsNullOrWhiteSpace(privateKeyPath))
                throw new ArgumentException("Invalid keyfile path", nameof(privateKeyPath));

            var handler = new DefaultUserJWTHandler(credentialsPath, privateKeyPath);
            UserJWTEventHandler = handler.DefaultUserJWTEventHandler;
            UserSignatureEventHandler = handler.DefaultUserSignatureHandler;
        }

        /// <summary>
        /// Sets user credentials using the NATS 2.0 security scheme.
        /// </summary>
        /// <param name="credentialsPath">A chained credentials file, e.g user.cred</param>
        public void SetUserCredentials(string credentialsPath)
        {
            if (string.IsNullOrWhiteSpace(credentialsPath))
                throw new ArgumentException("Invalid credentials path", nameof(credentialsPath));
            var handler = new DefaultUserJWTHandler(credentialsPath, credentialsPath);
            UserJWTEventHandler = handler.DefaultUserJWTEventHandler;
            UserSignatureEventHandler = handler.DefaultUserSignatureHandler;
        }

        /// <summary>
        /// SetUserJWT will set the callbacks to retrieve the user's JWT and
        /// the signature callback to sign the server nonce. This an the Nkey
        /// option are mutually exclusive.
        /// </summary>
        /// <param name="userJWTEventHandler">A User JWT Event Handler</param>
        /// <param name="userSignatureEventHandler">A User signature Event Handler</param>
        public void SetUserCredentialHandlers(EventHandler<UserJWTEventArgs> userJWTEventHandler,
            EventHandler<UserSignatureEventArgs> userSignatureEventHandler)
        {
            UserJWTEventHandler = userJWTEventHandler ?? throw new ArgumentNullException(nameof(userJWTEventHandler));
            UserSignatureEventHandler = userSignatureEventHandler ?? throw new ArgumentNullException(nameof(userSignatureEventHandler));
        }

        /// <summary>
        /// SetNkey will set the public Nkey and the signature callback to
        /// sign the server nonce.
        /// </summary>
        /// <param name="publicNkey">The User's public Nkey</param>
        /// <param name="userSignatureEventHandler">A User signature Event Handler to sign the server nonce.</param>
        public void SetNkey(string publicNkey, EventHandler<UserSignatureEventArgs> userSignatureEventHandler)
        {
            if (string.IsNullOrWhiteSpace(publicNkey))
                throw new ArgumentException("Invalid Nkey", "publicNkey");

            UserSignatureEventHandler = userSignatureEventHandler ?? throw new ArgumentNullException(nameof(userSignatureEventHandler));
            nkey = publicNkey;
        }

        /// <summary>
        /// SetNkey will set the public Nkey and the signature callback to
        /// sign the server nonce.
        /// </summary>
        /// <param name="publicNkey">The User's public Nkey</param>
        /// <param name="privateKeyPath">A path to a file contianing the private Nkey.</param>
        public void SetNkey(string publicNkey, string privateKeyPath)
        {
            if (string.IsNullOrWhiteSpace(publicNkey)) throw new ArgumentException("Invalid publicNkey", nameof(publicNkey));
            if (string.IsNullOrWhiteSpace(privateKeyPath)) throw new ArgumentException("Invalid filePath", nameof(privateKeyPath));

            nkey = publicNkey;
            UserSignatureEventHandler = (obj, args) =>
            {
                DefaultUserJWTHandler.SignNonceFromFile(privateKeyPath, args);
            };
        }

        /// <summary>
        /// Sets a custom JWT Event Handler and Signature handler.
        /// </summary>
        /// <param name="JWTEventHandler"></param>
        /// <param name="SignatureEventHandler"></param>
        public void SetJWTEventHandlers(EventHandler<UserJWTEventArgs> JWTEventHandler, EventHandler<UserSignatureEventArgs> SignatureEventHandler)
        {
            UserJWTEventHandler = JWTEventHandler ?? throw new ArgumentNullException("JWTEventHandler");
            UserSignatureEventHandler = SignatureEventHandler ?? throw new ArgumentNullException("SignatureEventHandler");
        }

        internal int maxPingsOut = Defaults.MaxPingOut;

        internal int subChanLen = 65536;
        internal int subscriberDeliveryTaskCount = 0;

        // Must be greater than 0.
        internal int subscriptionBatchSize = 64;

        internal string user;
        internal string password;
        internal string token;
        internal string nkey;

        // Options can only be publicly created through 
        // ConnectionFactory.GetDefaultOptions();
        internal Options() { }

        // Copy constructor
        internal Options(Options o)
        {
            allowReconnect = o.allowReconnect;
            AsyncErrorEventHandler = o.AsyncErrorEventHandler;
            ClosedEventHandler = o.ClosedEventHandler;
            ServerDiscoveredEventHandler = o.ServerDiscoveredEventHandler;
            DisconnectedEventHandler = o.DisconnectedEventHandler;
            UserJWTEventHandler = o.UserJWTEventHandler;
            UserSignatureEventHandler = o.UserSignatureEventHandler;
            maxPingsOut = o.maxPingsOut;
            maxReconnect = o.maxReconnect;
            name = o.name;
            noRandomize = o.noRandomize;
            noEcho = o.noEcho;
            pedantic = o.pedantic;
            useOldRequestStyle = o.useOldRequestStyle;
            pingInterval = o.pingInterval;
            ReconnectedEventHandler = o.ReconnectedEventHandler;
            reconnectWait = o.reconnectWait;
            secure = o.secure;
            user = o.user;
            password = o.password;
            token = o.token;
            nkey = o.nkey;
            verbose = o.verbose;
            subscriberDeliveryTaskCount = o.subscriberDeliveryTaskCount;
            subscriptionBatchSize = o.subscriptionBatchSize;

            if (o.url != null)
            {
                processUrlString(o.url);
            }
            
            if (o.servers != null)
            {
                servers = new string[o.servers.Length];
                Array.Copy(o.servers, servers, o.servers.Length);
            }

            subChanLen = o.subChanLen;
            timeout = o.timeout;
            TLSRemoteCertificationValidationCallback = o.TLSRemoteCertificationValidationCallback;

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
        /// Gets or sets the url used to connect to the NATs server.
        /// </summary>
        /// <remarks>
        /// This may contain username/password information.
        /// </remarks>
        public string Url
        {
            get { return url; }
            set { url = value; }
        }

        /// <summary>
        /// Gets or sets the array of servers that the NATs client will connect to.
        /// </summary>
        /// <remarks>
        /// The individual URLs may contain username/password information.
        /// </remarks>
        public string[] Servers
        {
            get { return servers; }
            set { servers = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether or not the server chosen for connection
        /// should not be selected randomly.
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
        /// Gets or sets a value indicating whether or not logging information should be verbose.
        /// </summary>
        public bool Verbose
        {
            get { return verbose;  }
            set { verbose = value; }
        }

        /// <summary>
        /// This option is not used by the NATS Client.
        /// </summary>
        public bool Pedantic
        {
            get { return pedantic; }
            set { pedantic = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether or not the old
        /// request pattern should be used.
        /// </summary>
        /// <remarks>
        /// The old request pattern involved a separate subscription
        /// per request inbox. The new style (default) involves creating
        /// a single inbox subscription per connection, upon the first
        /// request, and mapping outbound requests over that one
        /// subscription.
        /// </remarks>
        public bool UseOldRequestStyle
        {
            get { return useOldRequestStyle; }
            set { useOldRequestStyle = value; }
        }

        /// <summary>
        /// Get or sets a value indicating whether or not a secure connection (TLS)
        /// should be made to NATS servers.
        /// </summary>
        public bool Secure
        {
            get { return secure; }
            set { secure = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether or not an <see cref="IConnection"/> will attempt
        /// to reconnect to the NATS server if a connection has been lost.
        /// </summary>
        public bool AllowReconnect
        {
            get { return allowReconnect; }
            set { allowReconnect = value; }
        }


        /// <summary>
        /// Set <see cref="Options.MaxReconnect"/> to this value for the client to attempt to
        /// connect indefinitely. 
        /// </summary>
        public static int ReconnectForever = -1;

        /// <summary>
        /// Gets or sets the maxmimum number of times a connection will
        /// attempt to reconnect.  To reconnect indefinitely set this value to
        /// <see cref="Options.ReconnectForever"/>
        /// </summary>
        public int MaxReconnect
        {
            get { return maxReconnect; }
            set { maxReconnect = value; }
        }

        /// <summary>
        /// Gets or sets the amount of time, in milliseconds, the client will 
        /// wait before attempting a reconnection.
        /// </summary>
        public int ReconnectWait
        {
            get { return reconnectWait; }
            set { reconnectWait = value; }
        }

        /// <summary>
        /// Gets or sets the interval, in milliseconds, pings will be sent to the server.
        /// </summary>
        /// <remarks>
        /// Take care to coordinate this value with the server's interval.
        /// </remarks>
        public int PingInterval
        {
            get { return pingInterval; }
            set { pingInterval = value; }
        }

        /// <summary>
        /// Gets or sets the timeout, in milliseconds, when connecting to a NATS server.
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
        /// Gets or sets the user name used when connecting to the NATs server,
        /// when not included directly in the URLs.
        /// </summary>
        public string User
        {
            get { return user; }
            set { user = value; }
        }

        /// <summary>
        /// Sets the user password used when connecting to the NATs server,
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
        /// Adds an X.509 certifcate from a file for use with a secure connection.
        /// </summary>
        /// <param name="fileName">Path to the certificate file to add.</param>
        /// <exception cref="ArgumentNullException"><paramref name="fileName"/> is <c>null</c>.</exception>
        /// <exception cref="System.Security.Cryptography.CryptographicException">An error with the certificate
        /// ocurred. For example:
        /// <list>
        /// <item>The certificate file does not exist.</item>
        /// <item>The certificate is invalid.</item>
        /// <item>The certificate's password is incorrect.</item></list></exception>
        public void AddCertificate(string fileName)
        {
            if (fileName == null)
                throw new ArgumentNullException("fileName");
            X509Certificate2 cert = new X509Certificate2(fileName);
            AddCertificate(cert);
        }

        /// <summary>
        /// Adds an X.509 certifcate for use with a secure connection.
        /// </summary>
        /// <param name="certificate">An X.509 certificate represented as an <see cref="X509Certificate2"/> object.</param>
        /// <exception cref="ArgumentNullException"><paramref name="certificate"/> is <c>null</c>.</exception>
        /// <exception cref="System.Security.Cryptography.CryptographicException">An error with the certificate
        /// ocurred. For example:
        /// <list>
        /// <item>The certificate file does not exist.</item>
        /// <item>The certificate is invalid.</item>
        /// <item>The certificate's password is incorrect.</item></list></exception>
        public void AddCertificate(X509Certificate2 certificate)
        {
            if (certificate == null)
                throw new ArgumentNullException("certificate");
            if (certificates == null)
                certificates = new X509Certificate2Collection();

            certificates.Add(certificate);
        }

        /// <summary>
        /// Overrides the default NATS RemoteCertificationValidationCallback.
        /// </summary>
        /// <remarks>
        /// The default callback simply checks if there were any protocol
        /// errors. Overriding this callback is useful during testing, or accepting self
        /// signed certificates.
        /// </remarks>
        public RemoteCertificateValidationCallback TLSRemoteCertificationValidationCallback;


        /// <summary>
        /// Gets or sets the number of long running tasks to deliver messages
        /// to asynchronous subscribers. The default is zero (<c>0</c>) indicating each
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

        /// <summary>
        /// Gets or sets the batch size for calling subscription handlers.
        /// </summary>
        /// <remarks>
        /// When delivering messages to the subscriber, the batch size determines
        /// how many messages could be retrieved from the internal subscription
        /// queue at one time. This can allow higher performance from a single
        /// subscriber by avoiding the locking overhead of one-at-a-time
        /// retrieval from the queue.
        /// </remarks>
        public int SubscriptionBatchSize
        {
            get { return subscriptionBatchSize; }
            set
            {
                if (value <= 0)
                {
                    throw new ArgumentOutOfRangeException("value", "Subscription batch size must be greater than 0");
                }

                subscriptionBatchSize = value;
            }
        }

        /// <summary>
        /// NoEcho configures whether the server will echo back messages
        /// that are sent on this connection if we also have matching subscriptions.
        /// Note this is supported on servers >= version 1.2. Proto 1 or greater.
        /// </summary>
        public bool NoEcho { get => noEcho; set => noEcho = value; }

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
            sb.AppendFormat("Name={0};", Name != null ? Name : "null");
            sb.AppendFormat("NoRandomize={0};", NoRandomize);
            sb.AppendFormat("NoEcho={0};", NoEcho);
            sb.AppendFormat("Pendantic={0};", Pedantic);
            sb.AppendFormat("UseOldRequestStyle={0}", UseOldRequestStyle);
            sb.AppendFormat("PingInterval={0};", PingInterval);
            sb.AppendFormat("ReconnectWait={0};", ReconnectWait);
            sb.AppendFormat("Secure={0};", Secure);
            sb.AppendFormat("User={0};", User);
            sb.AppendFormat("Token={0};", Token);
            sb.AppendFormat("SubscriberDeliveryTaskCount={0};", SubscriberDeliveryTaskCount);
            sb.AppendFormat("SubscriptionBatchSize={0};", SubscriptionBatchSize);

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

