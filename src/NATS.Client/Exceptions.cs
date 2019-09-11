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

namespace NATS.Client
{
    /// <summary>
    /// The exception that is thrown when there is a NATS error condition.  All
    /// NATS exception inherit from this class.
    /// </summary>
    public class NATSException : Exception
    {
        internal NATSException() : base() { }
        internal NATSException(string err) : base (err) {}
        internal NATSException(string err, Exception innerEx) : base(err, innerEx) { }
    }

    /// <summary>
    /// The exception that is thrown when there is a connection error.
    /// </summary>
    public class NATSConnectionException : NATSException
    {
        internal NATSConnectionException(string err) : base(err) { }
        internal NATSConnectionException(string err, Exception innerEx) : base(err, innerEx) { }
    }

    /// <summary>
    /// This exception that is thrown when there is an internal error with
    /// the NATS protocol.
    /// </summary>
    public class NATSProtocolException : NATSException
    {
        internal NATSProtocolException(string err) : base(err) { }
    }

    /// <summary>
    /// The exception that is thrown when a connection cannot be made
    /// to any server.
    /// </summary>
    public class NATSNoServersException : NATSException
    {
        internal NATSNoServersException(string err) : base(err) { }
    }

    /// <summary>
    /// The exception that is thrown when a secure connection is requested,
    /// but not required.
    /// </summary>
    public class NATSSecureConnWantedException : NATSException
    {
        internal NATSSecureConnWantedException() : base("A secure connection is requested.") { }
    }

    /// <summary>
    /// The exception that is thrown when a secure connection is required.
    /// </summary>
    public class NATSSecureConnRequiredException : NATSException
    {
        internal NATSSecureConnRequiredException() : base("A secure connection is required.") { }
        internal NATSSecureConnRequiredException(String s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a an operation is performed on
    /// a connection that is closed.
    /// </summary>
    public class NATSConnectionClosedException : NATSException
    {
        internal NATSConnectionClosedException() : base("Connection is closed.") { }
    }

    /// <summary>
    /// The exception that is thrown when a consumer (subscription) is slow.
    /// </summary>
    public class NATSSlowConsumerException : NATSException
    {
        internal NATSSlowConsumerException() : base("Consumer is too slow.") { }
    }

    /// <summary>
    /// The exception that is thrown when an operation occurs on a connection
    /// that has been determined to be stale.
    /// </summary>
    public class NATSStaleConnectionException : NATSException
    {
        internal NATSStaleConnectionException() : base("Connection is stale.") { }
    }

    /// <summary>
    /// The exception that is thrown when a message payload exceeds what
    /// the maximum configured.
    /// </summary>
    public class NATSMaxPayloadException : NATSException
    {
        internal NATSMaxPayloadException() : base("Maximum payload size has been exceeded") { }
        internal NATSMaxPayloadException(string err) : base(err) { }
    }

    /// <summary>
    /// The exception that is thrown when a subscriber has exceeded the maximum
    /// number of messages that has been configured.
    /// </summary>
    public class NATSMaxMessagesException : NATSException
    {
        internal NATSMaxMessagesException() : base("Maximum number of messages have been exceeded.") { }
    }
    
    /// <summary>
    /// The exception that is thrown when a subscriber operation is performed on
    /// an invalid subscriber.
    /// </summary>
    public class NATSBadSubscriptionException : NATSException
    {
        internal NATSBadSubscriptionException() : base("Subscription is not valid.") { }
        internal NATSBadSubscriptionException(string s) : base("s") { }
    }

    /// <summary>
    /// The exception that is thrown when a NATS operation times out.
    /// </summary>
    public class NATSTimeoutException : NATSException
    {
        internal NATSTimeoutException() : base("Timeout occurred.") { }
        internal NATSTimeoutException(String s) : base(s) { }
    }

    /// <summary>
    /// The exception that is thrown when a NATS operation is performed on a draining connection.
    /// </summary>
    public class NATSConnectionDrainingException : NATSConnectionException
    {
        internal NATSConnectionDrainingException() : base("Connection is draining.") { }
    }
}