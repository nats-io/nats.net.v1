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
using System.Threading.Tasks;

namespace NATS.Client
{
    /// <summary>
    /// Represents a connection to a NATS Server which uses a client specified
    /// encoding scheme.
    /// </summary>
    public interface IEncodedConnection : IDisposable
    {
        /// <summary>
        /// Gets the configuration options for this instance.
        /// </summary>
        Options Opts { get; }

        /// <summary>
        /// Gets the URL of the NATS server to which this instance
        /// is connected, otherwise <c>null</c>.
        /// </summary>
        string ConnectedUrl { get; }

        /// <summary>
        /// Gets the server ID of the NATS server to which this instance
        /// is connected, otherwise <c>null</c>.
        /// </summary>
        string ConnectedId { get; }

        /// <summary>
        /// Gets an array of known server URLs for this instance.
        /// </summary>
        /// <remarks><see cref="Servers"/> also includes any additional
        /// servers discovered after a connection has been established. If
        /// authentication is enabled, <see cref="Options.User"/> or
        /// <see cref="Options.Token"/> must be used when connecting with
        /// these URLs.</remarks>
        string[] Servers { get; }

        /// <summary>
        /// Gets an array of server URLs that were discovered after this
        /// instance connected.
        /// </summary>
        /// <remarks>If authentication is enabled, <see cref="Options.User"/> or
        /// <see cref="Options.Token"/> must be used when connecting with
        /// these URLs.</remarks>
        string[] DiscoveredServers { get; }

        /// <summary>
        /// Gets the last <see cref="Exception"/> encountered by this instance,
        /// otherwise <c>null</c>.
        /// </summary>
        Exception LastError { get; }

        /// <summary>
        /// Publishes the serialized value of <paramref name="obj"/> to the given <paramref name="subject"/>.
        /// </summary>
        /// <param name="subject">The subject to publish <paramref name="obj"/> to over
        /// the current connection.</param>
        /// <param name="obj">The <see cref="Object"/> to serialize and publish to the connected NATS server.</param>
        /// <seealso cref="IConnection.Publish(string, byte[])"/>
        void Publish(string subject, object obj);

        /// <summary>
        /// Publishes the serialized value of <paramref name="obj"/> to the given <paramref name="subject"/>.
        /// </summary>
        /// <param name="subject">The subject to publish <paramref name="obj"/> to over
        /// the current connection.</param>
        /// <param name="reply">An optional reply subject.</param>
        /// <param name="obj">The <see cref="Object"/> to serialize and publish to the connected NATS server.</param>
        /// <seealso cref="IConnection.Publish(string, byte[])"/>
        void Publish(string subject, string reply, object obj);

        /// <summary>
        /// Sends a request payload and returns the deserialized response, or throws
        /// <see cref="NATSTimeoutException"/> if the <paramref name="timeout"/> expires.
        /// </summary>
        /// <remarks>
        /// <see cref="Request(string, object, int)"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="IEncodedConnection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="obj"/> to over
        /// the current connection.</param>
        /// <param name="obj">The <see cref="Object"/> to serialize and publish to the connected NATS server.</param>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        /// <returns>A <see cref="Object"/> with the deserialized response from the NATS server.</returns>
        /// <seealso cref="IConnection.Request(string, byte[])"/>
        object Request(string subject, object obj, int timeout);

        /// <summary>
        /// Sends a request payload and returns the deserialized response.
        /// </summary>
        /// <remarks>
        /// <see cref="Request(string, object)"/> will create an unique inbox for this request, sharing a single
        /// subscription for all replies to this <see cref="IEncodedConnection"/> instance. However, if 
        /// <see cref="Options.UseOldRequestStyle"/> is set, each request will have its own underlying subscription. 
        /// The old behavior is not recommended as it may cause unnecessary overhead on connected NATS servers.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="obj"/> to over
        /// the current connection.</param>
        /// <param name="obj">The <see cref="Object"/> to serialize and publish to the connected NATS server.</param>
        /// <returns>A <see cref="Object"/> with the deserialized response from the NATS server.</returns>
        /// <seealso cref="IConnection.Request(string, byte[])"/>
        object Request(string subject, object obj);

        /// <summary>
        /// Creates an inbox string which can be used for directed replies from subscribers.
        /// </summary>
        /// <remarks>
        /// The returned inboxes are guaranteed to be unique, but can be shared and subscribed
        /// to by others.
        /// </remarks>
        /// <returns>A unique inbox string.</returns>
        string NewInbox();

        /// <summary>
        /// Expresses interest in the given <paramref name="subject"/> to the NATS Server, and begins delivering
        /// messages to the given event handler.
        /// </summary>
        /// <remarks>The <see cref="IAsyncSubscription"/> returned will start delivering messages
        /// to the event handler as soon as they are received. The caller does not have to invoke
        /// <see cref="IAsyncSubscription.Start"/>.</remarks>
        /// <param name="subject">The subject on which to listen for messages.
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <param name="handler">The <see cref="EventHandler{TEventArgs}"/> invoked when messages are received 
        /// on the returned <see cref="IAsyncSubscription"/>.</param>
        /// <returns>An <see cref="IAsyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>.</returns>
        /// <seealso cref="ISubscription.Subject"/>
        IAsyncSubscription SubscribeAsync(string subject, EventHandler<EncodedMessageEventArgs> handler);

        /// <summary>
        /// Creates an asynchronous queue subscriber on the given <paramref name="subject"/>, and begins delivering
        /// messages to the given event handler.
        /// </summary>
        /// <remarks>The <see cref="IAsyncSubscription"/> returned will start delivering messages
        /// to the event handler as soon as they are received. The caller does not have to invoke
        /// <see cref="IAsyncSubscription.Start"/>.</remarks>
        /// <param name="subject">The subject on which to listen for messages.
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <param name="queue">The name of the queue group in which to participate.</param>
        /// <param name="handler">The <see cref="EventHandler{TEventArgs}"/> invoked when messages are received 
        /// on the returned <see cref="IAsyncSubscription"/>.</param>
        /// <returns>An <see cref="IAsyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>.</returns>
        /// <seealso cref="ISubscription.Subject"/>
        /// <seealso cref="ISubscription.Queue"/>
        IAsyncSubscription SubscribeAsync(string subject, string queue, EventHandler<EncodedMessageEventArgs> handler);

        /// <summary>
        /// Performs a round trip to the server and returns when it receives the internal reply, or throws
        /// a <see cref="NATSTimeoutException"/> exception if the NATS Server does not reply in time.
        /// </summary>
        /// <param name="timeout">The number of milliseconds to wait.</param>
        void Flush(int timeout);

        /// <summary>
        /// Performs a round trip to the server and returns when it receives the internal reply.
        /// </summary>
        void Flush();

        /// <summary>
        /// Immediately flushes the underlying connection buffer if the connection is valid.
        /// </summary>
        /// <exception cref="NATSConnectionClosedException">The <see cref="Connection"/> is closed.</exception>
        /// <exception cref="NATSException">There was an unexpected exception performing an internal NATS call while executing the
        /// request. See <see cref="Exception.InnerException"/> for more details.</exception>
        void FlushBuffer();

        /// <summary>
        /// Closes the <see cref="IConnection"/> and all associated
        /// subscriptions.
        /// </summary>
        /// <seealso cref="IsClosed"/>
        /// <seealso cref="State"/>
        void Close();

        /// <summary>
        /// Returns a value indicating whether or not the <see cref="IConnection"/>
        /// instance is closed.
        /// </summary>
        /// <returns><c>true</c> if and only if the <see cref="IConnection"/> is
        /// closed, otherwise <c>false</c>.</returns>
        /// <seealso cref="Close"/>
        /// <seealso cref="State"/>
        bool IsClosed();

        /// <summary>
        /// Returns a value indicating whether or not the <see cref="IConnection"/>
        /// is currently reconnecting.
        /// </summary>
        /// <returns><c>true</c> if and only if the <see cref="IConnection"/> is
        /// reconnecting, otherwise <c>false</c>.</returns>
        /// <seealso cref="State"/>
        bool IsReconnecting();

        /// <summary>
        /// Gets the current state of the <see cref="IConnection"/>.
        /// </summary>
        /// <seealso cref="ConnState"/>
        ConnState State { get; }

        /// <summary>
        /// Gets the statistics tracked for the <see cref="IConnection"/>.
        /// </summary>
        /// <seealso cref="ResetStats"/>
        IStatistics Stats { get; }

        /// <summary>
        /// Resets the associated statistics for the <see cref="IConnection"/>.
        /// </summary>
        /// <seealso cref="Stats"/>
        void ResetStats();

        /// <summary>
        /// Gets the maximum size in bytes of any payload sent
        /// to the connected NATS Server.
        /// </summary>
        /// <seealso cref="Publish(string, object)"/>
        /// <seealso cref="Publish(string, string, object)"/>
        /// <seealso cref="Request(string, object)"/>
        /// <seealso cref="Request(string, object, int)"/>
        long MaxPayload { get; }

        /// <summary>
        /// Gets or sets the method which is called to serialize
        /// objects sent as a message payload.
        /// </summary>
        Serializer OnSerialize { get; set; }

        /// <summary>
        /// Gets or sets the method which is called to deserialize
        /// objects from a message payload.
        /// </summary>
        Deserializer OnDeserialize { get; set; }

        /// <summary>
        /// Drains a connection for graceful shutdown.
        /// </summary>
        /// <remarks>
        /// Drain will put a connection into a drain state. All subscriptions will
        /// immediately be put into a drain state. Upon completion, the publishers
        /// will be drained and can not publish any additional messages. Upon draining
        /// of the publishers, the connection will be closed. Use the 
        /// <see cref="Options.ClosedEventHandler"/> option to know when the connection
        /// has moved from draining to closed.
        /// </remarks>
        /// <seealso cref="Close()"/>
        /// <returns>A task that represents the asynchronous drain operation.</returns>
        Task DrainAsync();

        /// <summary>
        /// Drains a connection for graceful shutdown.
        /// </summary>
        /// <remarks>
        /// Drain will put a connection into a drain state. All subscriptions will
        /// immediately be put into a drain state. Upon completion, the publishers
        /// will be drained and can not publish any additional messages. Upon draining
        /// of the publishers, the connection will be closed. Use the 
        /// <see cref="Options.ClosedEventHandler"/> option to know when the connection
        /// has moved from draining to closed.
        /// </remarks>
        /// <seealso cref="Close()"/>
        /// <param name="timeout">The duration to wait before draining.</param> 
        /// <returns>A task that represents the asynchronous drain operation.</returns>
        Task DrainAsync(int timeout);

        /// <summary>
        /// Drains a connection for graceful shutdown.
        /// </summary>
        /// <remarks>
        /// Drain will put a connection into a drain state. All subscriptions will
        /// immediately be put into a drain state. Upon completion, the publishers
        /// will be drained and can not publish any additional messages. Upon draining
        /// of the publishers, the connection will be closed. Use the 
        /// <see cref="Options.ClosedEventHandler"/> option to know when the connection
        /// has moved from draining to closed.
        /// </remarks>
        /// <seealso cref="Close()"/>
        void Drain();

        /// <summary>
        /// Drains a connection for graceful shutdown.
        /// </summary>
        /// <remarks>
        /// Drain will put a connection into a drain state. All subscriptions will
        /// immediately be put into a drain state. Upon completion, the publishers
        /// will be drained and can not publish any additional messages. Upon draining
        /// of the publishers, the connection will be closed. Use the 
        /// <see cref="Options.ClosedEventHandler"/> option to know when the connection
        /// has moved from draining to closed.
        /// </remarks>
        /// <seealso cref="Close()"/>
        /// <param name="timeout">The duration to wait before draining.</param> 
        void Drain(int timeout);

        /// <summary>
        /// Returns a value indicating whether or not the <see cref="IConnection"/>
        /// connection is draining.
        /// </summary>
        /// <returns><c>true</c> if and only if the <see cref="IConnection"/> is
        /// closed, otherwise <c>false</c>.</returns>
        /// <seealso cref="Close"/>
        /// <seealso cref="State"/>
        bool IsDraining();

        /// <summary>
        /// Get the number of active subscriptions.
        /// </summary>
        int SubscriptionCount { get; }
    }
}