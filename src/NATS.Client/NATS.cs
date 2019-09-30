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
//     - Minimal/no reliance on third party packages.
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

        /// <summary>
        /// Default subscriber pending messages limit.
        /// </summary>
        public const long SubPendingMsgsLimit = 65536;

        /// <summary>
        /// Default subscriber pending bytes limit.
        /// </summary>
        public const long SubPendingBytesLimit = 65536 * 1024;

        /// <summary>
        /// Default Drain Timeout in milliseconds.
        /// </summary>
        public const int DefaultDrainTimeout = 30000;

        /*
         * Namespace level defaults
         */

        // Scratch storage for assembling protocol headers
        internal const int scratchSize = 512;

        // The size of the bufio writer on top of the socket.
        internal const int defaultBufSize = 32768;

        // The read size from the network stream.
        internal const int defaultReadLength = 20480;

        // The size of the bufio while we are reconnecting
        internal const int defaultPendingSize = 1024 * 1024;

        // Default server pool size
        internal const int srvPoolSize = 4;
    }

    /// <summary>
    /// Provides the details when the state of a <see cref="Connection"/>
    /// changes.
    /// </summary>
    public class ConnEventArgs : EventArgs
    {
        private Connection c;   
            
        internal ConnEventArgs(Connection c)
        {
            this.c = c;
        }

        /// <summary>
        /// Gets the <see cref="Connection"/> associated with the event.
        /// </summary>
        public Connection Conn
        {
            get { return c; }
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
        /// Sets the JWT read by the event handler.   This MUST be set in the event handler.
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
                    throw new NATSConnectionException("SignedNonce was not set by the UserSignature event hander.");
                }
                return signedNonce;
            }
        }
    }

    /// <summary>
    /// Provides details for an error encountered asynchronously
    /// by an <see cref="IConnection"/>.
    /// </summary>
    public class ErrEventArgs : EventArgs
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
        /// Gets the <see cref="Connection"/> associated with the event.
        /// </summary>
        public Connection Conn
        {
            get { return c; }
        }

        /// <summary>
        /// Gets the <see cref="NATS.Client.Subscription"/> associated with the event.
        /// </summary>
        public Subscription Subscription
        {
            get { return s; }
        }

        /// <summary>
        /// Gets the error message associated with the event.
        /// </summary>
        public string Error
        {
            get { return err; }
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
        internal const string okProtoNoCRLF = "+OK";

        internal const string STALE_CONNECTION = "stale connection";
        internal const string AUTH_TIMEOUT = "authorization timeout";
    }

    /// <summary>
    /// Provides the message received by an <see cref="IAsyncSubscription"/>.
    /// </summary>
    public class MsgHandlerEventArgs : EventArgs
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

    // Borrowed from:  https://stackoverflow.com/a/7135008
    internal class Base32
    {
        public static byte[] Decode(string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                throw new ArgumentNullException("input");
            }

            input = input.TrimEnd('='); //remove padding characters
            int byteCount = input.Length * 5 / 8; //this must be TRUNCATED
            byte[] returnArray = new byte[byteCount];

            byte curByte = 0, bitsRemaining = 8;
            int mask = 0, arrayIndex = 0;

            foreach (char c in input)
            {
                int cValue = CharToValue(c);

                if (bitsRemaining > 5)
                {
                    mask = cValue << (bitsRemaining - 5);
                    curByte = (byte)(curByte | mask);
                    bitsRemaining -= 5;
                }
                else
                {
                    mask = cValue >> (5 - bitsRemaining);
                    curByte = (byte)(curByte | mask);
                    returnArray[arrayIndex++] = curByte;
                    curByte = (byte)(cValue << (3 + bitsRemaining));
                    bitsRemaining += 3;
                }
            }

            //if we didn't end with a full byte
            if (arrayIndex != byteCount)
            {
                returnArray[arrayIndex] = curByte;
            }

            return returnArray;
        }

        public static string Encode(byte[] input)
        {
            if (input == null || input.Length == 0)
            {
                throw new ArgumentNullException("input");
            }

            int charCount = (int)Math.Ceiling(input.Length / 5d) * 8;
            char[] returnArray = new char[charCount];

            byte nextChar = 0, bitsRemaining = 5;
            int arrayIndex = 0;

            foreach (byte b in input)
            {
                nextChar = (byte)(nextChar | (b >> (8 - bitsRemaining)));
                returnArray[arrayIndex++] = ValueToChar(nextChar);

                if (bitsRemaining < 4)
                {
                    nextChar = (byte)((b >> (3 - bitsRemaining)) & 31);
                    returnArray[arrayIndex++] = ValueToChar(nextChar);
                    bitsRemaining += 5;
                }

                bitsRemaining -= 3;
                nextChar = (byte)((b << bitsRemaining) & 31);
            }

            //if we didn't end with a full char
            if (arrayIndex != charCount)
            {
                returnArray[arrayIndex++] = ValueToChar(nextChar);
                while (arrayIndex != charCount) returnArray[arrayIndex++] = '='; //padding
            }

            return new string(returnArray);
        }

        private static int CharToValue(char c)
        {
            int value = (int)c;

            //65-90 == uppercase letters
            if (value < 91 && value > 64) return value - 65;

            //50-55 == numbers 2-7
            if (value < 56 && value > 49) return value - 24;

            //97-122 == lowercase letters
            if (value < 123 && value > 96) return value - 97;

            throw new ArgumentException("Character is not a Base32 character.", "c");
        }

        private static char ValueToChar(byte b)
        {
            if (b < 26) return (char)(b + 65);
            if (b < 32) return (char)(b + 24);
            throw new ArgumentException("Byte is not a value Base32 value.", "b");
        }
    }
}
