﻿// Copyright 2015-2018 The NATS Authors
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
using System.Net.Security;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using static NATS.Client.Defaults;

namespace NATS.Client
{
    /// <summary>
    /// This class is used to set up all NATs client options.
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
        bool ignoreDiscoveredServers = false;
        bool tlsFirst = false;
        private bool clientSideLimitChecks = true;
        IServerProvider serverProvider = null;
        int maxReconnect  = Defaults.MaxReconnect;
        int reconnectWait = Defaults.ReconnectWait;
        int pingInterval  = Defaults.PingInterval;
        int timeout       = Defaults.Timeout;
        int reconnectJitter = Defaults.ReconnectJitter;
        int reconnectJitterTLS = Defaults.ReconnectJitterTLS;
        ITCPConnection tcpConnection = null;

        internal X509Certificate2Collection certificates = null;

        private bool Equals(Options other)
        {
            return url == other.url 
                   && Equals(servers, other.servers)
                   && noRandomize == other.noRandomize 
                   && name == other.name
                   && verbose == other.verbose
                   && pedantic == other.pedantic 
                   && useOldRequestStyle == other.useOldRequestStyle 
                   && secure == other.secure 
                   && allowReconnect == other.allowReconnect 
                   && noEcho == other.noEcho 
                   && ignoreDiscoveredServers == other.ignoreDiscoveredServers 
                   && tlsFirst == other.tlsFirst 
                   && clientSideLimitChecks == other.clientSideLimitChecks 
                   && maxReconnect == other.maxReconnect 
                   && reconnectWait == other.reconnectWait 
                   && pingInterval == other.pingInterval 
                   && timeout == other.timeout 
                   && reconnectJitter == other.reconnectJitter 
                   && reconnectJitterTLS == other.reconnectJitterTLS 
                   && Equals(certificates, other.certificates)
                   && maxPingsOut == other.maxPingsOut 
                   && pendingMessageLimit == other.pendingMessageLimit 
                   && pendingBytesLimit == other.pendingBytesLimit 
                   && subscriberDeliveryTaskCount == other.subscriberDeliveryTaskCount 
                   && subscriptionBatchSize == other.subscriptionBatchSize 
                   && reconnectBufSize == other.reconnectBufSize 
                   && user == other.user 
                   && password == other.password 
                   && token == other.token 
                   && nkey == other.nkey 
                   && customInboxPrefix == other.customInboxPrefix 
                   && CheckCertificateRevocation == other.CheckCertificateRevocation;
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is Options other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (url != null ? url.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (servers != null ? servers.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ noRandomize.GetHashCode();
                hashCode = (hashCode * 397) ^ (name != null ? name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ verbose.GetHashCode();
                hashCode = (hashCode * 397) ^ pedantic.GetHashCode();
                hashCode = (hashCode * 397) ^ useOldRequestStyle.GetHashCode();
                hashCode = (hashCode * 397) ^ secure.GetHashCode();
                hashCode = (hashCode * 397) ^ allowReconnect.GetHashCode();
                hashCode = (hashCode * 397) ^ noEcho.GetHashCode();
                hashCode = (hashCode * 397) ^ ignoreDiscoveredServers.GetHashCode();
                hashCode = (hashCode * 397) ^ tlsFirst.GetHashCode();
                hashCode = (hashCode * 397) ^ clientSideLimitChecks.GetHashCode();
                hashCode = (hashCode * 397) ^ (serverProvider != null ? serverProvider.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ maxReconnect;
                hashCode = (hashCode * 397) ^ reconnectWait;
                hashCode = (hashCode * 397) ^ pingInterval;
                hashCode = (hashCode * 397) ^ timeout;
                hashCode = (hashCode * 397) ^ reconnectJitter;
                hashCode = (hashCode * 397) ^ reconnectJitterTLS;
                hashCode = (hashCode * 397) ^ (tcpConnection != null ? tcpConnection.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (certificates != null ? certificates.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ClosedEventHandler != null ? ClosedEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ServerDiscoveredEventHandler != null ? ServerDiscoveredEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (DisconnectedEventHandler != null ? DisconnectedEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ReconnectedEventHandler != null ? ReconnectedEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (AsyncErrorEventHandler != null ? AsyncErrorEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (LameDuckModeEventHandler != null ? LameDuckModeEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ReconnectDelayHandler != null ? ReconnectDelayHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (HeartbeatAlarmEventHandler != null ? HeartbeatAlarmEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (UnhandledStatusEventHandler != null ? UnhandledStatusEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (PullStatusWarningEventHandler != null ? PullStatusWarningEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (PullStatusErrorEventHandler != null ? PullStatusErrorEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (FlowControlProcessedEventHandler != null ? FlowControlProcessedEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (UserJWTEventHandler != null ? UserJWTEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (UserSignatureEventHandler != null ? UserSignatureEventHandler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ maxPingsOut;
                hashCode = (hashCode * 397) ^ pendingMessageLimit.GetHashCode();
                hashCode = (hashCode * 397) ^ pendingBytesLimit.GetHashCode();
                hashCode = (hashCode * 397) ^ subscriberDeliveryTaskCount;
                hashCode = (hashCode * 397) ^ subscriptionBatchSize;
                hashCode = (hashCode * 397) ^ reconnectBufSize;
                hashCode = (hashCode * 397) ^ (user != null ? user.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (password != null ? password.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (token != null ? token.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (nkey != null ? nkey.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (customInboxPrefix != null ? customInboxPrefix.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (TLSRemoteCertificationValidationCallback != null ? TLSRemoteCertificationValidationCallback.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ CheckCertificateRevocation.GetHashCode();
                return hashCode;
            }
        }

        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when a connection is closed.
        /// </summary>
        public EventHandler<ConnEventArgs> ClosedEventHandler;
        public EventHandler<ConnEventArgs> ClosedEventHandlerOrDefault => ClosedEventHandler ?? DefaultClosedEventHandler();

        /// <summary>
        /// Represents the method that will handle an event raised
        /// whenever a new server has joined the cluster.
        /// </summary>
        public EventHandler<ConnEventArgs> ServerDiscoveredEventHandler;
        public EventHandler<ConnEventArgs> ServerDiscoveredEventHandlerOrDefault => ServerDiscoveredEventHandler ?? DefaultServerDiscoveredEventHandler();

        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when a connection has been disconnected from a server.
        /// </summary>
        public EventHandler<ConnEventArgs> DisconnectedEventHandler;
        public EventHandler<ConnEventArgs> DisconnectedEventHandlerOrDefault => DisconnectedEventHandler ?? DefaultDisconnectedEventHandler();

        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when a connection has reconnected to a server.
        /// </summary>
        public EventHandler<ConnEventArgs> ReconnectedEventHandler;
        public EventHandler<ConnEventArgs> ReconnectedEventHandlerOrDefault => ReconnectedEventHandler ?? DefaultReconnectedEventHandler();

        /// <summary>
        /// Represents the method that will handle an event raised 
        /// when an error occurs out of band.
        /// </summary>
        public EventHandler<ErrEventArgs> AsyncErrorEventHandler;
        public EventHandler<ErrEventArgs> AsyncErrorEventHandlerOrDefault => AsyncErrorEventHandler ?? DefaultAsyncErrorEventHandler();

        /// <summary>
        /// Represents the method that will handle an event raised
        /// when the server notifies the connection that it entered lame duck mode.
        /// </summary>
        /// <remarks>
        /// A server in lame duck mode will gradually disconnect all its connections
        /// before shutting down. This is often used in deployments when upgrading
        /// NATS Servers.
        /// </remarks>
        public EventHandler<ConnEventArgs> LameDuckModeEventHandler;
        public EventHandler<ConnEventArgs> LameDuckModeEventHandlerOrDefault => LameDuckModeEventHandler ?? DefaultLameDuckModeEventHandler();

        /// <summary>
        /// Represents the optional method that is used to get from the
        /// user the desired delay the client should pause before attempting
        /// to reconnect again.
        /// </summary>
        /// <remarks>
        /// Note that this is invoked after the library tried the
        /// entire list of URLs and failed to reconnect.  By default, the client
        /// will use the sum of <see cref="ReconnectWait"/> and a random value between
        /// zero and <see cref="Options.ReconnectJitter"/> or
        /// <see cref="Options.ReconnectJitterTLS"/>
        /// </remarks>
        public EventHandler<ReconnectDelayEventArgs> ReconnectDelayHandler;

        /// <summary>
        /// Represents the method that will handle a heartbeat alarm
        /// </summary>
        public EventHandler<HeartbeatAlarmEventArgs> HeartbeatAlarmEventHandler;
        public EventHandler<HeartbeatAlarmEventArgs> HeartbeatAlarmEventHandlerOrDefault => HeartbeatAlarmEventHandler ?? DefaultHeartbeatAlarmEventHandler();

        /// <summary>
        /// Represents the method that will handle an unhandled status received in a push subscription.
        /// </summary>
        public EventHandler<UnhandledStatusEventArgs> UnhandledStatusEventHandler;
        public EventHandler<UnhandledStatusEventArgs> UnhandledStatusEventHandlerOrDefault => UnhandledStatusEventHandler ?? DefaultUnhandledStatusEventHandler();

        /// <summary>
        /// Represents the method that will handle a status message that indicating either the subscription or pull might be problematic.
        /// </summary>
        public EventHandler<StatusEventArgs> PullStatusWarningEventHandler;
        public EventHandler<StatusEventArgs> PullStatusWarningEventHandlerOrDefault => PullStatusWarningEventHandler ?? DefaultPullStatusWarningEventHandler();

        /// <summary>
        /// Represents the method that will handle a status message that indicating either the subscription cannot continue or the pull request cannot be processed.
        /// </summary>
        public EventHandler<StatusEventArgs> PullStatusErrorEventHandler;
        public EventHandler<StatusEventArgs> PullStatusErrorEventHandlerOrDefault => PullStatusErrorEventHandler ?? DefaultPullStatusErrorEventHandler();

        /// <summary>
        /// Represents the method that will handle a flow control processed event
        /// </summary>
        public EventHandler<FlowControlProcessedEventArgs> FlowControlProcessedEventHandler;
        public EventHandler<FlowControlProcessedEventArgs> FlowControlProcessedEventHandlerOrDefault => FlowControlProcessedEventHandler ?? DefaultFlowControlProcessedEventHandler();
        
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
        /// Sets user credentials from text instead of a file using the NATS 2.0 security scheme.
        /// </summary>
        /// <param name="credentialsText">The text containing the "-----BEGIN NATS USER JWT-----" block
        /// and the text containing the "-----BEGIN USER NKEY SEED-----" block</param>
        public void SetUserCredentialsFromString(string credentialsText)
        {
            var handler = new StringUserJWTHandler(credentialsText, credentialsText);
            UserJWTEventHandler = handler.DefaultUserJWTEventHandler;
            UserSignatureEventHandler = handler.DefaultUserSignatureHandler;
        }

        /// <summary>
        /// Sets user credentials from text instead of a file using the NATS 2.0 security scheme.
        /// </summary>
        /// <param name="userJwtText">The text containing the "-----BEGIN NATS USER JWT-----" block</param>
        /// <param name="nkeySeedText">The text containing the "-----BEGIN USER NKEY SEED-----" block or the seed beginning with "SU".
        /// Both strings can contain the credentials and the jwt block since they will be parsed for the required data.</param>
        public void SetUserCredentialsFromStrings(string userJwtText, string nkeySeedText)
        {
            var handler = new StringUserJWTHandler(userJwtText, nkeySeedText);
            UserJWTEventHandler = handler.DefaultUserJWTEventHandler;
            UserSignatureEventHandler = handler.DefaultUserSignatureHandler;
        }

        /// <summary>
        /// Sets user credentials using the NATS 2.0 security scheme.
        /// </summary>
        /// <param name="credentialsPath">A chained credentials file, e.g. user.cred</param>
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
        /// the signature callback to sign the server nonce. This and the Nkey
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
                throw new ArgumentException("Invalid Nkey", nameof(publicNkey));

            UserSignatureEventHandler = userSignatureEventHandler ?? throw new ArgumentNullException(nameof(userSignatureEventHandler));
            nkey = publicNkey;
        }

        /// <summary>
        /// SetNkey will set the public Nkey and the signature callback to
        /// sign the server nonce.
        /// </summary>
        /// <param name="publicNkey">The User's public Nkey</param>
        /// <param name="privateKeyPath">A path to a file containing the private Nkey.</param>
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
            UserJWTEventHandler = JWTEventHandler ?? throw new ArgumentNullException(nameof(JWTEventHandler));
            UserSignatureEventHandler = SignatureEventHandler ?? throw new ArgumentNullException(nameof(SignatureEventHandler));
        }

        internal int maxPingsOut = MaxPingOut;

        private long pendingMessageLimit = SubPendingMsgsLimit;
        private long pendingBytesLimit = SubPendingBytesLimit;
        internal int subscriberDeliveryTaskCount = 0;

        // Must be greater than 0.
        internal int subscriptionBatchSize = 64;
        internal int reconnectBufSize = Defaults.ReconnectBufferSize;

        internal string user;
        internal string password;
        internal string token;
        internal string nkey;

        internal string customInboxPrefix;

        // Options can only be publicly created through 
        // ConnectionFactory.GetDefaultOptions();
        internal Options() { }

        // Copy constructor
        internal Options(Options o)
        {
            allowReconnect = o.allowReconnect;
            AsyncErrorEventHandler = o.AsyncErrorEventHandler;
            HeartbeatAlarmEventHandler = o.HeartbeatAlarmEventHandler;
            UnhandledStatusEventHandler = o.UnhandledStatusEventHandler;
            PullStatusWarningEventHandler = o.PullStatusWarningEventHandler;
            PullStatusErrorEventHandler = o.PullStatusErrorEventHandler;
            FlowControlProcessedEventHandler = o.FlowControlProcessedEventHandler;
            ClosedEventHandler = o.ClosedEventHandler;
            ServerDiscoveredEventHandler = o.ServerDiscoveredEventHandler;
            DisconnectedEventHandler = o.DisconnectedEventHandler;
            UserJWTEventHandler = o.UserJWTEventHandler;
            UserSignatureEventHandler = o.UserSignatureEventHandler;
            ReconnectDelayHandler = o.ReconnectDelayHandler;
            LameDuckModeEventHandler = o.LameDuckModeEventHandler;
            maxPingsOut = o.maxPingsOut;
            maxReconnect = o.maxReconnect;
            name = o.name;
            noRandomize = o.noRandomize;
            noEcho = o.noEcho;
            ignoreDiscoveredServers = o.ignoreDiscoveredServers;
            tlsFirst = o.tlsFirst;
            clientSideLimitChecks = o.clientSideLimitChecks;
            serverProvider = o.serverProvider;
            pedantic = o.pedantic;
            reconnectBufSize = o.reconnectBufSize;
            useOldRequestStyle = o.useOldRequestStyle;
            pingInterval = o.pingInterval;
            ReconnectedEventHandler = o.ReconnectedEventHandler;
            reconnectJitter = o.reconnectJitter;
            reconnectJitterTLS = o.reconnectJitterTLS;
            reconnectWait = o.reconnectWait;
            secure = o.secure;
            user = o.user;
            password = o.password;
            token = o.token;
            nkey = o.nkey;
            verbose = o.verbose;
            subscriberDeliveryTaskCount = o.subscriberDeliveryTaskCount;
            subscriptionBatchSize = o.subscriptionBatchSize;
            customInboxPrefix = o.customInboxPrefix;
            tcpConnection = o.TCPConnection;

            url = o.url;
            if (o.servers != null)
            {
                servers = new string[o.servers.Length];
                Array.Copy(o.servers, servers, o.servers.Length);
            }

            pendingMessageLimit = o.pendingMessageLimit;
            pendingBytesLimit = o.pendingBytesLimit;
            timeout = o.timeout;
            TLSRemoteCertificationValidationCallback = o.TLSRemoteCertificationValidationCallback;

            if (o.certificates != null)
            {
                certificates = new X509Certificate2Collection(o.certificates);
            }

            CheckCertificateRevocation = o.CheckCertificateRevocation;
        }

        static readonly string[] protcolSep = new[] {"://"};
        
        static string ensureProperUrl(string url)
        {
            if (string.IsNullOrWhiteSpace(url))
                return url;
            
            if (url.StartsWith("nats://", StringComparison.OrdinalIgnoreCase))
                return url;

            if (url.StartsWith("tls://", StringComparison.OrdinalIgnoreCase))
                return url;

            var parts = url.Split(protcolSep, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 1)
                return $"nats://{url.Trim()}";
            
            throw new ArgumentException("Allowed protocols are: 'nats://, tls://'.");
        }

        /// <summary>
        /// Gets or sets the url used to connect to the NATs server.
        /// Can be a comma delimited list, it will be turning into Servers
        /// </summary>
        /// <remarks>
        /// This may contain username/password information.
        /// </remarks>
        public string Url
        {
            get => url;
            set
            {
                if (value == null)
                {
                    url = null;
                    servers = null;
                }
                else 
                {
                    Servers = value.Split(',');
                }
            }
        }

        /// <summary>
        /// Gets or sets the array of servers that the NATS client will connect to.
        /// </summary>
        /// <remarks>
        /// The individual URLs may contain username/password information.
        /// </remarks>
        public string[] Servers
        {
            get { return servers; }
            set
            {
                if (value == null || value.Length == 0)
                {
                    servers = null;
                    url = null;
                }
                else
                {
                    servers = new string[value.Length];
                    for (int i = 0; i < value.Length; i++)
                    {
                        servers[i] = ensureProperUrl(value[i]);
                    }
                    url = string.Join(",", servers);
                }
            }
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
            get { return tlsFirst || secure; }
            set { secure = value; }
        }

        /// <summary>
        /// Get or sets a custom ITCPConnection object to use for communication
        /// to the NATs Server.
        /// </summary>
        public ITCPConnection TCPConnection
        {
            get { return tcpConnection; }
            set { tcpConnection = value; }
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
        /// Gets or sets the maximum number of times a connection will
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
	                    nameof(value),
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
        [Obsolete("Please use the PendingMessageLimit property instead")]
        public int SubChannelLength
        {
            get { return (int)PendingMessageLimit; }
            set { PendingMessageLimit = value; }
        }

        /// <summary>
        /// Gets or sets the size of the subscriber channel, or number
        /// of messages the subscriber will buffer internally.
        /// </summary>
        public long PendingMessageLimit
        {
            get { return pendingMessageLimit; }
            set { pendingMessageLimit = value; }
        }

        /// <summary>
        /// Gets or sets the maximum pending bytes limit for the subscription, 
        /// or number of bytes the subscriber will buffer internally.
        /// </summary>
        public long PendingBytesLimit
        {
            get { return pendingBytesLimit; }
            set { pendingBytesLimit = value; }
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
        /// Gets or sets a custom inbox prefix.
        /// </summary>
        public string CustomInboxPrefix
        {
            get => customInboxPrefix;
            set
            {
                if (value != null && !Subscription.IsValidPrefix(value))
                    throw new ArgumentException("Prefix would result in an invalid subject.");

                customInboxPrefix = value;
            }
        }

        /// <summary>
        /// Adds an X.509 certificate from a file for use with a secure connection.
        /// </summary>
        /// <param name="fileName">Path to the certificate file to add.</param>
        /// <exception cref="ArgumentNullException"><paramref name="fileName"/> is <c>null</c>.</exception>
        /// <exception cref="System.Security.Cryptography.CryptographicException">An error with the certificate
        /// occurred. For example:
        /// <list>
        /// <item>The certificate file does not exist.</item>
        /// <item>The certificate is invalid.</item>
        /// <item>The certificate's password is incorrect.</item></list></exception>
        public void AddCertificate(string fileName)
        {
            if (fileName == null)
                throw new ArgumentNullException(nameof(fileName));
            X509Certificate2 cert = new X509Certificate2(fileName);
            AddCertificate(cert);
        }

        /// <summary>
        /// Adds an X.509 certificate for use with a secure connection.
        /// </summary>
        /// <param name="certificate">An X.509 certificate represented as a <see cref="X509Certificate2"/> object.</param>
        /// <exception cref="ArgumentNullException"><paramref name="certificate"/> is <c>null</c>.</exception>
        /// <exception cref="System.Security.Cryptography.CryptographicException">An error with the certificate
        /// occurred. For example:
        /// <list>
        /// <item>The certificate file does not exist.</item>
        /// <item>The certificate is invalid.</item>
        /// <item>The certificate's password is incorrect.</item></list></exception>
        public void AddCertificate(X509Certificate2 certificate)
        {
            if (certificate == null)
                throw new ArgumentNullException(nameof(certificate));
            if (certificates == null)
                certificates = new X509Certificate2Collection();

            certificates.Add(certificate);
        }

        /// <summary>
        /// Clears all X.509 certificates added through <see cref="AddCertificate(string)"/>.
        /// </summary>
        /// <remarks>
        /// Can be used to renew the client certificate without recreating the <see cref="Connection"/>.
        /// </remarks>
        public void ClearCertificates()
        {
	        certificates?.Clear();
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
                    throw new ArgumentOutOfRangeException(
	                    nameof(value),
	                    "SubscriberDeliveryTaskCount must be 0 or greater.");
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
                    throw new ArgumentOutOfRangeException(
	                    nameof(value),
	                    "Subscription batch size must be greater than 0");
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

        /// <summary>
        /// Whether or not to ignore discovered servers when considering for connect/reconnect
        /// </summary>
        public bool IgnoreDiscoveredServers { get => ignoreDiscoveredServers; set => ignoreDiscoveredServers = value; }
        
        /// <summary>
        /// Whether or not to do Tls Handshake First. Valid against servers 2.10.3 and later
        /// </summary>
        public bool TlsFirst { get => tlsFirst; set => tlsFirst = value; }

        /// <summary>
        /// Whether or not to make client side limit checks, currently only core publish/request max payload
        /// </summary>
        public bool ClientSideLimitChecks { get => clientSideLimitChecks; set => clientSideLimitChecks = value; }

        internal IServerProvider ServerProvider { get => serverProvider; set => serverProvider = value; }
        
        private void appendEventHandler(StringBuilder sb, string name, Delegate eh)
        {
            if (eh != null)
                sb.AppendFormat("{0}={1};", name, eh.GetMethodInfo().Name);
            else
                sb.AppendFormat("{0}=null;", name);
        }

        
        /// <summary>
        /// Constant used to sets the reconnect buffer size to unbounded.
        /// </summary>
        /// <seealso cref="ReconnectBufferSize"/>
        public static readonly int ReconnectBufferSizeUnbounded = 0;

        /// <summary>
        /// Constant that disables the reconnect buffer.
        /// </summary>
        /// <seealso cref="ReconnectBufferSize"/>
        public static readonly int ReconnectBufferDisabled = -1;

        /// <summary>
        /// Gets or sets the buffer size of messages kept while busy reconnecting.
        /// </summary>
        /// <remarks>
        /// When reconnecting, the NATS client will hold published messages that
        /// will be flushed to the new server upon a successful reconnect.  The default
        /// is buffer size is 8 MB.  This buffering can be disabled.
        /// </remarks>
        /// <seealso cref="ReconnectBufferSizeUnbounded"/>
        /// <seealso cref="ReconnectBufferDisabled"/>
        public int ReconnectBufferSize
        {
            get { return reconnectBufSize; }
            set
            {
                if (value < -1)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Reconnect buffer size must be greater than or equal to -1");
                }

                reconnectBufSize = value;
            }
        }

        /// <summary>
        /// Sets the upper bound for a random delay in milliseconds added to
        /// ReconnectWait during a reconnect for clear and TLS connections.
        /// </summary>
        /// <remarks>
        /// Defaults are 100 ms and 1s for TLS.
        /// </remarks>
        /// <seealso cref="ReconnectDelayHandler"/>
        /// <seealso cref="ReconnectJitter"/>
        /// <seealso cref="ReconnectJitterTLS"/>
        /// <seealso cref="ReconnectWait"/>
        public void SetReconnectJitter(int jitter, int tlsJitter)
        {
            if (jitter < 0 || tlsJitter < 0)
            {
                throw new ArgumentOutOfRangeException("value", "Reconnect jitter must be positive");
            }

            reconnectJitter = jitter;
            reconnectJitterTLS = tlsJitter;
        }

        /// <summary>
        /// Get the upper bound for a random delay added to
        /// ReconnectWait during a reconnect for connections.
        /// </summary>
        /// <seealso cref="ReconnectDelayHandler"/>
        /// <seealso cref="ReconnectJitterTLS"/>
        /// <seealso cref="ReconnectWait"/>
        /// <seealso cref="SetReconnectJitter(int, int)"/>
        public int ReconnectJitter { get => reconnectJitter; }


        /// <summary>
        /// Get the upper bound for a random delay added to
        /// ReconnectWait during a reconnect for TLS connections.
        /// </summary>
        /// <seealso cref="ReconnectDelayHandler"/>
        /// <seealso cref="ReconnectJitter"/>
        /// <seealso cref="SetReconnectJitter(int, int)"/>
        public int ReconnectJitterTLS { get => reconnectJitterTLS; }

        /// <summary>
        /// Get or set whether to check Certificate Revocation when connecting via TLS
        /// </summary>
        /// <seealso cref="SslStream.AuthenticateAsClientAsync(string, X509CertificateCollection, System.Security.Authentication.SslProtocols, bool)"/>
        public bool CheckCertificateRevocation { get; set; } = true;

        /// <summary>
        /// Returns a string representation of the
        /// value of this Options instance.
        /// </summary>
        /// <returns>string value of this instance.</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("{");
            sb.AppendFormat("AllowReconnect={0};", allowReconnect);

            appendEventHandler(sb, "AsyncErrorEventHandler", AsyncErrorEventHandler);
            appendEventHandler(sb, "HeartbeatAlarmEventHandler", HeartbeatAlarmEventHandler);
            appendEventHandler(sb, "UnhandledStatusEventHandler", UnhandledStatusEventHandler);
            appendEventHandler(sb, "PullStatusWarningEventHandler", PullStatusWarningEventHandler);
            appendEventHandler(sb, "PullStatusErrorEventHandler", PullStatusErrorEventHandler);
            appendEventHandler(sb, "FlowControlProcessedEventHandler", FlowControlProcessedEventHandler);
            appendEventHandler(sb, "ClosedEventHandler", ClosedEventHandler);
            appendEventHandler(sb, "DisconnectedEventHandler", DisconnectedEventHandler);
            appendEventHandler(sb, "ReconnectedEventHandler", ReconnectedEventHandler);
            appendEventHandler(sb, "ReconnectDelayHandler", ReconnectDelayHandler);
            appendEventHandler(sb, "ServerDiscoveredEventHandler", ServerDiscoveredEventHandler);
            appendEventHandler(sb, "LameDuckModeEventHandler", LameDuckModeEventHandler);

            sb.AppendFormat("MaxPingsOut={0};", MaxPingsOut);
            sb.AppendFormat("MaxReconnect={0};", MaxReconnect);
            sb.AppendFormat("Name={0};", Name != null ? Name : "null");
            sb.AppendFormat("NoRandomize={0};", NoRandomize);
            sb.AppendFormat("NoEcho={0};", NoEcho);
            sb.AppendFormat("IgnoreDiscoveredServers={0};", ignoreDiscoveredServers);
            sb.AppendFormat("TlsFirst={0};", tlsFirst);
            sb.AppendFormat("clientSideLimitChecks={0};", clientSideLimitChecks);
            sb.AppendFormat("ServerProvider={0};", serverProvider == null ? "Default" : "Provided");
            sb.AppendFormat("Pedantic={0};", Pedantic);
            sb.AppendFormat("UseOldRequestStyle={0};", UseOldRequestStyle);
            sb.AppendFormat("PingInterval={0};", PingInterval);
            sb.AppendFormat("ReconnectBufferSize={0};", ReconnectBufferSize);
            sb.AppendFormat("ReconnectJitter={0};", ReconnectJitter);
            sb.AppendFormat("ReconnectJitterTLS={0};", ReconnectJitterTLS);
            sb.AppendFormat("ReconnectWait={0};", ReconnectWait);
            sb.AppendFormat("Secure={0};", Secure);
            sb.AppendFormat("SubscriberDeliveryTaskCount={0};", SubscriberDeliveryTaskCount);
            sb.AppendFormat("SubscriptionBatchSize={0};", SubscriptionBatchSize);
            sb.AppendFormat("User={0};", User);
            sb.AppendFormat("Token={0};", Token);

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
                sb.Append("};");
            }
            sb.AppendFormat($"{nameof(PendingMessageLimit)}={PendingMessageLimit};");
            sb.AppendFormat($"{nameof(PendingBytesLimit)}={PendingBytesLimit};");
            sb.AppendFormat("Timeout={0}", Timeout);
            sb.Append("}");

            return sb.ToString();
        }
    }
}

