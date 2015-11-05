// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


// This is the NATS .NET client.
// 
// This Apcera supported client library follows the go client closely, 
// diverging where it makes sense to follow the common design 
// semantics of the language.
// 
// While public and protected methods 
// and properties adhere to the .NET coding guidlines, 
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
//     - A minimal/no reliance on third party packages.
//
//     Coding guidelines are based on:
//     http://blogs.msdn.com/b/brada/archive/2005/01/26/361363.aspx
//     although method location mirrors the go client to faciliate
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
        public const string Version = "0.0.1";

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

        /*
         * Namespace level defaults
         */

	    // Scratch storage for assembling protocol headers
	    internal const int scratchSize = 512;

	    // The size of the bufio reader/writer on top of the socket.
        // .NET perform better with small buffer sizes.
        internal const int defaultBufSize    = 32512;
        internal const int defaultReadLength = 512;

	    // The size of the bufio while we are reconnecting
        internal const int defaultPendingSize = 1024 * 1024;

	    // Default server pool size
        internal const int srvPoolSize = 4;
    }

    /// <summary>
    /// Event arguments for the ConnEventHandler type delegate.
    /// </summary>
    public class ConnEventArgs
    {
        private Connection c;   
            
        internal ConnEventArgs(Connection c)
        {
            this.c = c;
        }

        /// <summary>
        /// Gets the connection associated with the event.
        /// </summary>
        public Connection Conn
        {
            get { return c; }
        }
    }

    /// <summary>
    /// Event arguments for the ErrorEventHandler type delegate.
    /// </summary>
    public class ErrEventArgs
    {
        private Connection c;
        private Subscription s;
        private String err;

        internal ErrEventArgs(Connection c, Subscription s, String err)
        {
            this.c = c;
            this.s = s;
            this.err = err;
        }

        /// <summary>
        /// Gets the connection associated with the event.
        /// </summary>
        public Connection Conn
        {
            get { return c; }
        }

        /// <summary>
        /// Gets the Subscription associated wit the event.
        /// </summary>
        public Subscription Subscription
        {
            get { return s; }
        }

        /// <summary>
        /// Gets the error associated with the event.
        /// </summary>
        public string Error
        {
            get { return err; }
        }

    }

    /// <summary>
    /// Delegate to handle a connection related event.
    /// </summary>
    /// <param name="sender">Sender object.</param>
    /// <param name="e">Event arguments</param>
    public delegate void ConnEventHandler(object sender, ConnEventArgs e);

    /// <summary>
    /// Delegate to handle error events.
    /// </summary>
    /// <param name="sender">Sender object.</param>
    /// <param name="e">Sender object.</param>
    public delegate void ErrorEventHandler(object sender, ErrEventArgs e);

    /**
     * Internal Constants
     */
    internal class IC
    {
        internal const string _CRLF_  = "\r\n";
        internal const string _EMPTY_ = "";
        internal const string _SPC_   = " ";
        internal const string _PUB_P_ = "PUB ";

        internal const string _OK_OP_   = "+OK";
        internal const string _ERR_OP_  = "-ERR";
        internal const string _MSG_OP_  = "MSG";
        internal const string _PING_OP_ = "PING";
        internal const string _PONG_OP_ = "PONG";
        internal const string _INFO_OP_ = "INFO";

        internal const string inboxPrefix = "_INBOX.";

        internal const string conProto   = "CONNECT {0}" + IC._CRLF_;
        internal const string pingProto  = "PING" + IC._CRLF_;
        internal const string pongProto  = "PONG" + IC._CRLF_;
        internal const string pubProto   = "PUB {0} {1} {2}" + IC._CRLF_;
        internal const string subProto   = "SUB {0} {1} {2}" + IC._CRLF_;
        internal const string unsubProto = "UNSUB {0} {1}" + IC._CRLF_;

        internal const string pongProtoNoCRLF = "PONG"; 

        internal const string STALE_CONNECTION = "Stale Connection";
    }

    /// <summary>
    /// This class is passed into the MsgHandler delegate, providing the
    /// message received.
    /// </summary>
    public class MsgHandlerEventArgs
    {
        internal Msg msg = null;

        /// <summary>
        /// Retrieves the message.
        /// </summary>
        public Msg Message
        {
            get { return msg; }
        }
    }
   
    /// <summary>
    /// This delegate handles event raised when a message arrives.
    /// </summary>
    /// <param name="sender">Sender object.</param>
    /// <param name="args">MsgHandlerEventArgs</param>
    public delegate void MsgHandler(object sender, MsgHandlerEventArgs args);
}
