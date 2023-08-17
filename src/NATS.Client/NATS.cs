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
using System.Reflection;
using System.Text;
using NATS.Client.JetStream;

/*! \mainpage %NATS .NET client.
 *
 * \section intro_sec Introduction
 *
 * The %NATS .NET Client is part of %NATS an open-source, cloud-native
 * messaging system.
 * This client, written in C#, follows the go client closely, but
 * diverges in places to follow the common design semantics of a .NET API.
 *
 * \section install_sec Installation
 *
 * Instructions to build and install the %NATS .NET C# client can be
 * found at the [NATS .NET C# GitHub page](https://github.com/nats-io/csharp-nats)
 *
 * \section other_doc_section Other Documentation
 *
 * This documentation focuses on the %NATS .NET C# Client API; for additional
 * information, refer to the following:
 *
 * - [General Documentation for nats.io](https://nats-io.github.io/docs/)
 * - [NATS .NET C# Client found on GitHub](https://github.com/nats-io/nats.net)
 * - [The NATS server found on GitHub](https://github.com/nats-io/nats-server)
 */

// Notes on the NATS .NET client.
// 
// While public and protected methods 
// and properties adhere to the .NET coding guidelines, 
// internal/private members and methods mirror the go client for
// maintenance purposes.  Public method and properties are
// documented with standard .NET doc.
// 
//     - Public/Protected members and methods are in PascalCase
//     - Public/Protected members and methods are documented in 
//       standard .NET documentation.
//     - Private/Internal members and methods are in camelCase.
//     - There are no "callbacks" - delegates only.
//     - Public members are accessed through a property.
//     - When possible, internal members are accessed directly.
//     - Internal Variable Names mirror those of the go client.
//     - Minimal/no reliance on third party packages.
//
//     Coding guidelines are based on:
//     http://blogs.msdn.com/b/brada/archive/2005/01/26/361363.aspx
//     although method location mirrors the go client to facilitate
//     maintenance.
//     
namespace NATS.Client
{
    /// <summary>
    /// This class contains default values for fields used throughout NATS.
    /// </summary>
    public static class Defaults
    {
        /// <summary>
        /// Client version
        /// </summary>
        public static readonly string Version = typeof(Defaults).GetTypeInfo().Assembly.GetName().Version.ToString();

        /// <summary>
        /// The default NATS connect url ("nats://localhost:4222")
        /// </summary>
        public const string Url     = "nats://localhost:4222";

        /// <summary>
        /// The default NATS connect port. (4222)
        /// </summary>
        public const int    Port    = 4222;

        /// <summary>
        /// Default number of times to attempt a reconnect. (60)
        /// </summary>
        public const int    MaxReconnect = 60;

        /// <summary>
        /// Default ReconnectWait time (2 seconds)
        /// </summary>
        public const int    ReconnectWait  = 2000; // 2 seconds.
        
        /// <summary>
        /// Default timeout  (2 seconds).
        /// </summary>
        public const int    Timeout        = 2000; // 2 seconds.

        /// <summary>
        ///  Default ping interval (2 minutes);
        /// </summary>
        public const int    PingInterval   = 120000;// 2 minutes.

        /// <summary>
        /// Default MaxPingOut value (2);
        /// </summary>
        public const int    MaxPingOut     = 2;

        /// <summary>
        /// Default MaxChanLen (65536)
        /// </summary>
        public const int    MaxChanLen     = 65536;

        /// <summary>
        /// Default Request Channel Length
        /// </summary>
        public const int    RequestChanLen = 4;

        /// <summary>
        /// Language string of this client, ".NET"
        /// </summary>
        public const string LangString     = ".NET";

        /// <summary>
        /// Default subscriber pending messages limit.
        /// </summary>
        public const long SubPendingMsgsLimit = 512 * 1024;

        /// <summary>
        /// Default subscriber pending bytes limit.
        /// </summary>
        public const long SubPendingBytesLimit = 64 * 1024 * 1024;

        /// <summary>
        /// Default Drain Timeout in milliseconds.
        /// </summary>
        public const int DefaultDrainTimeout = 30000;

        /// <summary>
        /// Default Pending buffer size is 8 MB.
        /// </summary>
        public const int ReconnectBufferSize = 8 * 1024 * 1024; // 8MB

        /// <summary>
        /// Default non-TLS reconnect jitter of 100ms.
        /// </summary>
        public const int ReconnectJitter = 100;

        /// <summary>
        /// Default TLS reconnect jitter of 1s.
        /// </summary>
        public const int ReconnectJitterTLS = 1000;

        /*
         * Namespace level defaults
         */

        // Scratch storage for assembling protocol headers
        internal const int MaxControlLineSize = 4096;

        // The size of the bufio writer on top of the socket.
        public const int defaultBufSize = 32768;

        // The read size from the network stream.
        internal const int defaultReadLength = 20480;

        // The size of the bufio while we are reconnecting
        internal const int defaultPendingSize = 1024 * 1024;

        // Default server pool size
        internal const int srvPoolSize = 4;
        
        public static EventHandler<ConnEventArgs> DefaultClosedEventHandler() => 
            (sender, e) => WriteEvent("ClosedEvent", e);
        
        public static EventHandler<ConnEventArgs> DefaultServerDiscoveredEventHandler() => 
            (sender, e) => WriteEvent("ServerDiscoveredEvent", e);
        
        public static EventHandler<ConnEventArgs> DefaultDisconnectedEventHandler() => 
            (sender, e) => WriteEvent("DisconnectedEvent", e);

        public static EventHandler<ConnEventArgs> DefaultReconnectedEventHandler() => 
            (sender, e) => WriteEvent("ReconnectedEvent", e);

        public static EventHandler<ConnEventArgs> DefaultLameDuckModeEventHandler() => 
            (sender, e) => WriteEvent("LameDuckModeEvent", e);

        public static EventHandler<ErrEventArgs> DefaultAsyncErrorEventHandler() => 
            (sender, e) => WriteError("AsyncErrorEvent", e);

        public static EventHandler<HeartbeatAlarmEventArgs> DefaultHeartbeatAlarmEventHandler() =>
            (sender, e) =>
                WriteJsEvent("HeartbeatAlarm", e,
                    "lastStreamSequence: ", e?.LastStreamSequence ?? 0U,
                    "lastConsumerSequence: ", e?.LastConsumerSequence ?? 0U);

        public static EventHandler<UnhandledStatusEventArgs> DefaultUnhandledStatusEventHandler() => 
            (sender, e) => WriteJsEvent("UnhandledStatus", e, "Status: ", e?.Status);

        public static EventHandler<StatusEventArgs> DefaultPullStatusWarningEventHandler() => 
            (sender, e) => WriteJsEvent("PullStatusWarning", e, "Status: ", e?.Status);

        public static EventHandler<StatusEventArgs> DefaultPullStatusErrorEventHandler() => 
            (sender, e) => WriteJsEvent("PullStatusError", e, "Status: ", e?.Status);

        public static EventHandler<FlowControlProcessedEventArgs> DefaultFlowControlProcessedEventHandler() =>
            (sender, e) => WriteJsEvent("FlowControlProcessed", e, "FcSubject: ", e?.FcSubject, "Source: ", e?.Source);

        private static void WriteJsEvent(string label, ConnJsSubEventArgs e, params object[] pairs) {
            var sb = BeginFormatMessage(label, e?.Conn, e?.Sub, null);
            if (e?.JetStreamSub != null && e?.JetStreamSub.Consumer != null)
            {
                sb.Append(", ConsumerName:").Append(e.JetStreamSub.Consumer);
            }

            for (int x = 0; x < pairs.Length; x++) {
                sb.Append(", ").Append(pairs[x]).Append(pairs[++x]);
            }
            Console.Out.WriteLine(sb.ToString());
        }

        private static void WriteEvent(string label, ConnEventArgs e)
        {
            if (e == null)
                Console.Out.WriteLine(label);
            else if (e?.Error == null)
                Console.Out.WriteLine(BeginFormatMessage(label, e.Conn, null, null).ToString());
            else
                Console.Error.WriteLine(BeginFormatMessage(label, e.Conn, null, e.Error.Message).ToString());
        }

        private static void WriteError(string label, ErrEventArgs e) {
            Console.Error.WriteLine(e == null ? label
                : BeginFormatMessage(label, e.Conn, e.Subscription, e.Error).ToString());
        }

        private static StringBuilder BeginFormatMessage(string label, Connection conn, Subscription sub, string error)
        {
            StringBuilder sb = new StringBuilder(label);
            if (conn != null)
            {
                sb.Append(", Connection: ").Append(conn.ClientID);
            }
            if (sub != null) {
                sb.Append(", Subscription: ").Append(sub.Sid);
            }
            if (error != null)
            {
                sb.Append(", Error: ").Append(error);
            }
            return sb;
        }
    }

    public enum FlowControlSource { FlowControl, Heartbeat }

    /// <summary>
    /// Provides the details when the state of a <see cref="Connection"/>
    /// changes.
    /// </summary>
    public class ConnEventArgs : EventArgs
    {
        internal ConnEventArgs(Connection c, Exception error = null)
        {
            this.Conn = c;
            this.Error = error;
        }

        /// <summary>
        /// Gets the <see cref="Connection"/> associated with the event.
        /// </summary>
        public Connection Conn { get; }

        /// <summary>
        /// Gets any Exception associated with the connection state change.
        /// </summary>
        /// <example>Could be an exception causing the connection to get disconnected.</example>
        public Exception Error { get; }
    }

    /// <summary>
    /// Provides details for the ReconnectDelayEvent.
    /// </summary>
    /// <remarks>
    /// This event handler is a good place to apply backoff logic.  The associated
    /// connection will be RECONNECTING so accessing or calling IConnection methods will result
    /// in undefined behavior (including deadlocks).  Assigning a non-default handler
    /// requires the application to define reconnect delay and backoff behavior.
    /// </remarks>
    public class ReconnectDelayEventArgs : EventArgs
    {
        internal ReconnectDelayEventArgs(int attempts)
        {
            Attempts = attempts;
        }

        /// <Summary>
        /// Gets the number of times the client has traversed the
        /// server list in attempting to reconnect.
        /// </Summary>
        public int Attempts { get; }
    }

    /// <summary>
    /// Provides details for an error encountered asynchronously
    /// by an <see cref="IConnection"/>.
    /// </summary>
    public class ErrEventArgs : EventArgs
    {
        /// <summary>
        /// Gets the <see cref="Connection"/> associated with the event.
        /// </summary>
        public Connection Conn { get; }

        /// <summary>
        /// Gets the <see cref="NATS.Client.Subscription"/> associated with the event.
        /// </summary>
        public Subscription Subscription  { get; }
        
        /// <summary>
        /// Gets the error message associated with the event.
        /// </summary>
        public String Error { get; }

        public ErrEventArgs(Connection conn, Subscription subscription, string error)
        {
            Conn = conn;
            Subscription = subscription;
            Error = error;
        }
    }

    /// <summary>
    /// Base class for Event Args that have a connection and a subscription
    /// by an <see cref="IConnection"/>.
    /// </summary>
    public class ConnJsSubEventArgs : EventArgs
    {
        /// <summary>
        /// The <see cref="Connection"/> associated with the event.
        /// </summary>
        public Connection Conn { get; }
        
        /// <summary>
        /// The <see cref="NATS.Client.Subscription"/> associated with the event.
        /// </summary>
        public Subscription Sub { get; }

        /// <summary>
        /// The <see cref="NATS.Client.JetStream.IJetStreamSubscription"/> when the associated Subscription is of this type.
        /// </summary>
        public IJetStreamSubscription JetStreamSub { get; } 

        protected ConnJsSubEventArgs(Connection conn, Subscription sub)
        {
            Conn = conn;
            Sub = sub;
            if (sub is IJetStreamSubscription)
            {
                JetStreamSub = (IJetStreamSubscription)sub;
            }
        }
    }
    
    /// <summary>
    /// Provides details for an heartbeat alarm encountered
    /// </summary>
    public class HeartbeatAlarmEventArgs : ConnJsSubEventArgs
    {
        public ulong LastStreamSequence { get; }
        public ulong LastConsumerSequence { get; }

        public HeartbeatAlarmEventArgs(Connection c, Subscription s, 
            ulong lastStreamSequence, ulong lastConsumerSequence) : base(c, s)
        {
            LastStreamSequence = lastStreamSequence;
            LastConsumerSequence = lastConsumerSequence;
        }
    }

    /// <summary>
    /// Provides details for an status message when it is unknown or unhandled
    /// </summary>
    public class StatusEventArgs : ConnJsSubEventArgs
    {
        public MsgStatus Status { get; }

        public StatusEventArgs(Connection c, Subscription s, MsgStatus status) : base(c, s)
        {
            Status = status;
        }
    }

    /// <summary>
    /// Provides details for an status message when it is unknown or unhandled
    /// </summary>
    public class UnhandledStatusEventArgs : StatusEventArgs
    {
        public UnhandledStatusEventArgs(Connection c, Subscription s, MsgStatus status) : base(c, s, status) {}
    }
    
    /// <summary>
    /// Provides details for an status message when when a flow control is processed.
    /// </summary>
    public class FlowControlProcessedEventArgs : ConnJsSubEventArgs
    {
        public string FcSubject { get; }
        public FlowControlSource Source { get; }

        public FlowControlProcessedEventArgs(Connection c, Subscription s, string fcSubject, FlowControlSource source) : base(c, s)
        {
            FcSubject = fcSubject;
            Source = source;
        }
    }

    /// <summary>
    /// Provides details when a user JWT is read during a connection.  The
    /// JWT must be set or a <see cref="NATSConnectionException"/> will
    /// be thrown.
    /// </summary>
    public class UserJWTEventArgs : EventArgs
    {
        private string jwt = null;

        /// <Summary>
        /// Sets the JWT read by the event handler. This MUST be set in the event handler.
        /// </Summary>
        public string JWT 
        {
            set { jwt = value; }
            internal get
            {
                if (jwt == null) {
                    throw new NATSConnectionException("JWT was not set in the UserJWT event hander.");
                }
                return jwt;
            }
        }
    }

    /// <summary>
    /// Provides details when a user signature is read during a connection.
    /// The User Signature event signs the ServerNonce and sets the 
    /// SignedNonce with the result.
    /// The SignedNonce must be set or a <see cref="NATSConnectionException"/>
    /// will be thrown.
    /// </summary>
    public class UserSignatureEventArgs : EventArgs
    {
        private byte[] signedNonce = null;
        private byte[] serverNonce = null;


        internal UserSignatureEventArgs(byte[] nonce)
        {
            serverNonce = nonce;
        }

        /// <summary>
        /// Gets the nonce sent from the server.
        /// </summary>
        public byte[] ServerNonce
        {
            get { return serverNonce; }
        }

        /// <Summary>
        /// Sets the signed nonce to be returned to the server.  This MUST be set.
        /// </Summary>
        public byte[] SignedNonce
        {
            set { signedNonce = value; }
            internal get
            {
                if (signedNonce == null)
                {
                    throw new NATSConnectionException("SignedNonce was not set by the UserSignature event handler.");
                }
                return signedNonce;
            }
        }
    }

    /**
     * Internal Constants
     */
    internal class IC
    {
        internal const string _CRLF_  = "\r\n";
        internal const string _EMPTY_ = "";
        internal const string _SPC_   = " ";
        internal const string _PUB_P_ = "PUB ";
        internal const string _HPUB_P_ = "HPUB ";

        internal const string _OK_OP_   = "+OK";
        internal const string _ERR_OP_  = "-ERR";
        internal const string _MSG_OP_  = "MSG";
        internal const string _PING_OP_ = "PING";
        internal const string _PONG_OP_ = "PONG";
        internal const string _INFO_OP_ = "INFO";

        internal const string inboxPrefix = "_INBOX.";

        internal const string conProtoNoCRLF   = "CONNECT";
        internal const string pingProto  = "PING" + IC._CRLF_;
        internal const string pongProto  = "PONG" + IC._CRLF_;
        internal const string pubProto   = "PUB {0} {1} {2}" + IC._CRLF_;
        internal const string subProto   = "SUB {0} {1} {2}" + IC._CRLF_;
        internal const string unsubProto = "UNSUB {0} {1}" + IC._CRLF_;

        internal const string pongProtoNoCRLF = "PONG";
        internal const string okProtoNoCRLF = "+OK";

        internal const string STALE_CONNECTION = "stale connection";
        internal const string AUTH_TIMEOUT = "authorization timeout";
    }

    /// <summary>
    /// Provides the message received by an <see cref="IAsyncSubscription"/>.
    /// </summary>
    public class MsgHandlerEventArgs : EventArgs
    {
        public MsgHandlerEventArgs(Msg message)
        {
            Message = message;
        }
    

        /// <summary>
        /// Retrieves the message.
        /// </summary>
        public Msg Message { get; }
    }

    // Borrowed from:  https://stackoverflow.com/a/7135008
}
