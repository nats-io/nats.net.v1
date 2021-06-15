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
using NATS.Client.JetStream;

namespace NATS.Client
{
    /// <summary>
    /// The exception that is thrown when there is a NATS error condition.  All
    /// NATS exception inherit from this class.
    /// </summary>
    public class NATSException : Exception
    {
        public NATSException() : base() { }
        public NATSException(string err) : base (err) {}
        public NATSException(string err, Exception innerEx) : base(err, innerEx) { }
    }

    /// <summary>
    /// The exception that is thrown when there is a connection error.
    /// </summary>
    public class NATSConnectionException : NATSException
    {
        public NATSConnectionException(string err) : base(err) { }
        public NATSConnectionException(string err, Exception innerEx) : base(err, innerEx) { }
    }

    internal static class NATSConnectionExceptionExtensions
    {
        internal static bool IsAuthorizationViolationError(this NATSConnectionException ex)
            => ex?.Message.Equals("'authorization violation'", StringComparison.OrdinalIgnoreCase) == true;
        
        internal static bool IsAuthenticationExpiredError(this NATSConnectionException ex)
            => ex?.Message.Equals("'authentication expired'", StringComparison.OrdinalIgnoreCase) == true;
    }

    /// <summary>
    /// The exception that is thrown when there is an error writing
    /// to the internal reconnect buffer.
    /// </summary>
    public class NATSReconnectBufferException : NATSConnectionException
    {
        public NATSReconnectBufferException(string err) : base(err) { }
    }

    /// <summary>
    /// This exception that is thrown when there is an internal error with
    /// the NATS protocol.
    /// </summary>
    public class NATSProtocolException : NATSException
    {
        public NATSProtocolException(string err) : base(err) { }
    }

    /// <summary>
    /// The exception that is thrown when a connection cannot be made
    /// to any server.
    /// </summary>
    public class NATSNoServersException : NATSException
    {
        public NATSNoServersException(string err) : base(err) { }
    }

    /// <summary>
    /// The exception that is thrown when a secure connection is requested,
    /// but not required.
    /// </summary>
    public class NATSSecureConnWantedException : NATSException
    {
        public NATSSecureConnWantedException() : base("A secure connection is requested.") { }
    }

    /// <summary>
    /// The exception that is thrown when a secure connection is required.
    /// </summary>
    public class NATSSecureConnRequiredException : NATSException
    {
        public NATSSecureConnRequiredException() : base("A secure connection is required.") { }
        public NATSSecureConnRequiredException(string s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a an operation is performed on
    /// a connection that is closed.
    /// </summary>
    public class NATSConnectionClosedException : NATSException
    {
        public NATSConnectionClosedException() : base("Connection is closed.") { }
    }

    /// <summary>
    /// The exception that is thrown when a consumer (subscription) is slow.
    /// </summary>
    public class NATSSlowConsumerException : NATSException
    {
        public NATSSlowConsumerException() : base("Consumer is too slow.") { }
    }

    /// <summary>
    /// The exception that is thrown when an operation occurs on a connection
    /// that has been determined to be stale.
    /// </summary>
    public class NATSStaleConnectionException : NATSException
    {
        public NATSStaleConnectionException() : base("Connection is stale.") { }
    }

    /// <summary>
    /// The exception that is thrown when a message payload exceeds what
    /// the maximum configured.
    /// </summary>
    public class NATSMaxPayloadException : NATSException
    {
        public NATSMaxPayloadException() : base("Maximum payload size has been exceeded") { }
        public NATSMaxPayloadException(string err) : base(err) { }
    }

    /// <summary>
    /// The exception that is thrown when a subscriber has exceeded the maximum
    /// number of messages that has been configured.
    /// </summary>
    public class NATSMaxMessagesException : NATSException
    {
        public NATSMaxMessagesException() : base("Maximum number of messages have been exceeded.") { }
    }
    
    /// <summary>
    /// The exception that is thrown when a subscriber operation is performed on
    /// an invalid subscriber.
    /// </summary>
    public class NATSBadSubscriptionException : NATSException
    {
        public NATSBadSubscriptionException() : base("Subscription is not valid.") { }
        public NATSBadSubscriptionException(string s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a NATS operation times out.
    /// </summary>
    public class NATSTimeoutException : NATSException
    {
        public NATSTimeoutException() : base("Timeout occurred.") { }
        public NATSTimeoutException(string s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a NATS operation is not supported due
    /// to client and server feature conflict.
    /// </summary>
    public class NATSNotSupportedException : NATSException
    {
        public NATSNotSupportedException() : base("Operation not supported.") { }
        public NATSNotSupportedException(string s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a NATS header is invalid.
    /// </summary>
    public class NATSInvalidHeaderException : NATSException
    {
        public NATSInvalidHeaderException() : base("Invalid message header.") { }
        public NATSInvalidHeaderException(string s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a NATS operation is performed on a draining connection.
    /// </summary>
    public class NATSConnectionDrainingException : NATSConnectionException
    {
        public NATSConnectionDrainingException() : base("Connection is draining.") { }
    }

    /// <summary>
    /// The exception thrown when the server has detected there are no responders for a request.
    /// </summary>
    /// <remarks>
    /// This is circuit breaking behavior from the NATS server to more quickly identify when
    /// a request would have timed out.
    /// </remarks>
    public class NATSNoRespondersException : NATSTimeoutException
    {
        public NATSNoRespondersException() : base("No responders are available for the request.") { }
    }
}