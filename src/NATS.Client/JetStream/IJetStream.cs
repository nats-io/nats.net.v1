// Copyright 2021 The NATS Authors
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

namespace NATS.Client.JetStream
{
    /// <summary>
    /// This is the JetStream context for creation and access to persistent
    /// streams and consumers.
    /// </summary>
    /// <remarks>
    /// A JetStream context is created by the IConnect.JetStreamContext() API.
    /// </remarks>
    public interface IJetStream
    {
        /// <summary>
        /// Send a message to the specified subject and waits for a response
        /// from Jetstream. The message body will not be copied.
        /// </summary>
        /// <remarks>
        /// The expected usage with string content is something like:
        /// <code>
        /// nc = Nats.connect()
        /// IJetStream js = nc.JetStream()
        /// js.Publish("destination", "message".getBytes("UTF-8"), publishOptions)
        /// </code>
        /// where the sender creates a byte array immediately before calling publish.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="byte"/> that contains the data to publish
        /// to the connected NATS server.</param>
        /// <returns>PublishAck</returns>
        PublishAck Publish(string subject, byte[] data);

        /// <summary>
        /// Send a message to the specified subject and waits for a response
        /// from Jetstream. The message body will not be copied.
        /// </summary>
        /// <remarks>
        /// The expected usage with string content is something like:
        /// <code>
        /// nc = Nats.connect()
        /// IJetStream js = nc.JetStream
        /// js.Publish("destination", "message".getBytes("UTF-8"), publishOptions)
        /// </code>
        /// where the sender creates a byte array immediately before calling publish.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="byte"/> that contains the data to publish
        /// to the connected NATS server.</param>
        /// <param name="publishOptions">Options for publishing.</param>
        /// <returns>PublishAck</returns>
        PublishAck Publish(string subject, byte[] data, PublishOptions publishOptions);

        /// <summary>
        /// Send a message and waits for a response from JetStream.
        /// </summary>
        /// <param name="message">A <see cref="Msg"/> that contains the request data to publish
        /// to the connected NATS server.  Any reply subject will be ignored.</param>
        /// <returns>A publish Acknowedgement</returns>
        PublishAck Publish(Msg message);

        /// <summary>
        /// Send a message and waits for a response from JetStream.
        /// </summary>
        /// <param name="message">A <see cref="Msg"/> that contains the request data to publish
        /// to the connected NATS server.  Any reply subject will be ignored.</param>
        /// <param name="publishOptions">Options for publishing.</param>
        /// <returns>A publish acknowedgement.</returns>
        PublishAck Publish(Msg message, PublishOptions publishOptions);

        /// <summary>
        /// Asynchronously sends a message to the specified subject and waits for a response
        /// from Jetstream. The message body will not be copied.
        /// </summary>
        /// <remarks>
        /// The expected usage with string content is something like:
        /// <code>
        /// nc = Nats.connect()
        /// IJetStream js = nc.JetStream()
        /// js.Publish("destination", "message".getBytes("UTF-8"), publishOptions)
        /// </code>
        /// where the sender creates a byte array immediately before calling publish.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="byte"/> that contains the data to publish
        /// to the connected NATS server.</param>
        /// <returns>PublishAck</returns>
        Task<PublishAck> PublishAsync(string subject, byte[] data);

        /// <summary>
        /// Asynchronously sends data to the specified subject. The message
        /// body will not be copied.
        /// </summary>
        /// <remarks>
        /// The expected usage with string content is something like:
        /// <code>
        /// nc = Nats.connect()
        /// IJetStream js = nc.JetStream
        /// js.Publish("destination", "message".getBytes("UTF-8"), publishOptions)
        /// </code>
        /// where the sender creates a byte array immediately before calling publish.
        /// </remarks>
        /// <param name="subject">The subject to publish <paramref name="data"/> to over
        /// the current connection.</param>
        /// <param name="data">An array of type <see cref="byte"/> that contains the data to publish
        /// to the connected NATS server.</param>
        /// <param name="publishOptions">Options for publishing.</param>
        /// <returns>PublishAck</returns>
        Task<PublishAck> PublishAsync(string subject, byte[] data, PublishOptions publishOptions);

        /// FIX Comments for rest of async

        /// <summary>
        /// Asynchronously sends a message to JetStream.
        /// </summary>
        /// <param name="message">A <see cref="Msg"/> that contains the request data to publish
        /// to the connected NATS server.  Any reply subject will be ignored.</param>
        /// <returns>A publish Acknowedgement</returns>
        Task<PublishAck> PublishAsync(Msg message);

        /// <summary>
        /// Asynchronously sends a message to JetStream with options.
        /// </summary>
        /// <param name="message">A <see cref="Msg"/> that contains the request data to publish
        /// to the connected NATS server.  Any reply subject will be ignored.</param>
        /// <param name="publishOptions">Options for publishing.</param>
        /// <returns>A publish acknowedgement.</returns>
        Task<PublishAck> PublishAsync(Msg message, PublishOptions publishOptions);

        /// <summary>
        /// Creates a JetStream pull subscription.  Pull subscriptions fetch messages
        /// from the server in batches.
        /// </summary>
        /// <param name="subject">The subject on which to listen for messages.
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <param name="options">Pull Subcribe options for this subscription.</param>
        /// <returns>a JetStreamPullSubscription</returns>
        IJetStreamPullSubscription PullSubscribe(string subject, PullSubscribeOptions options);

        /// <summary>
        /// Creates a push subscriber on the given <paramref name="subject"/>, and begins delivering
        /// messages to the given event handler.
        /// </summary>
        /// <remarks>
        /// <para>All subscribers with the same queue name will form the queue group and
        /// only one member of the group will be selected to receive any given message.</para>
        /// <para>The <see cref="IJetStreamSubscription"/> returned will start delivering messages
        /// to the event handler as soon as they are received.</para>
        /// </remarks>
        /// <param name="subject">The subject on which to listen for messages.
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <param name="handler">The <see cref="EventHandler{MsgHandlerEventArgs}"/> invoked when messages are received 
        /// on the returned <see cref="IAsyncSubscription"/>.</param>
        /// <param name="autoAck">Whether or not to auto ack the message</param>
        /// <returns>An <see cref="IAsyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>.</returns>
        /// <seealso cref="ISubscription.Subject"/>
        /// <returns>A JetStream push subscription</returns>
        IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, EventHandler<MsgHandlerEventArgs> handler, bool autoAck);

        /// <summary>
        /// Creates a push subscriber on the given <paramref name="subject"/>, and begins delivering
        /// messages to the given event handler.
        /// </summary>
        /// <remarks>
        /// <para>All subscribers with the same queue name will form the queue group and
        /// only one member of the group will be selected to receive any given message.</para>
        /// <para>The <see cref="IJetStreamSubscription"/> returned will start delivering messages
        /// to the event handler as soon as they are received.</para>
        /// </remarks>
        /// <param name="subject">The subject on which to listen for messages.
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <param name="handler">The <see cref="EventHandler{MsgHandlerEventArgs}"/> invoked when messages are received 
        /// on the returned <see cref="IJetStreamPushAsyncSubscription"/>.</param>
        /// <param name="autoAck">Whether or not to auto ack the message</param>
        /// <param name="options">Pull Subcribe options for this subscription.</param>
        /// <returns>An <see cref="IJetStreamPushAsyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>.</returns>
        /// <seealso cref="ISubscription.Subject"/>
        IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, EventHandler<MsgHandlerEventArgs> handler, bool autoAck, PushSubscribeOptions options);

        /// <summary>
        /// Creates an subscriber on the given <paramref name="subject"/>, and begins delivering
        /// messages to the given event handler.
        /// </summary>
        /// <remarks>
        /// <para>All subscribers with the same queue name will form the queue group and
        /// only one member of the group will be selected to receive any given message.</para>
        /// <para>The <see cref="IJetStreamSubscription"/> returned will start delivering messages
        /// to the event handler as soon as they are received.</para>
        /// </remarks>
        /// <param name="subject">The subject on which to listen for messages.
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <param name="queue">The name of the queue group in which to participate.</param>
        /// <param name="handler">The <see cref="EventHandler{MsgHandlerEventArgs}"/> invoked when messages are received 
        /// on the returned <see cref="IAsyncSubscription"/>.</param>
        /// <param name="autoAck">Whether or not to auto ack the message</param>
        /// <returns>An <see cref="IAsyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>.</returns>
        /// <seealso cref="ISubscription.Subject"/>
        /// <seealso cref="ISubscription.Queue"/>
        /// <returns>A JetStream push subscription</returns>
        IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler, bool autoAck);

        /// <summary>
        /// Creates an subscriber on the given <paramref name="subject"/>, and begins delivering
        /// messages to the given event handler.
        /// </summary>
        /// <remarks>
        /// <para>All subscribers with the same queue name will form the queue group and
        /// only one member of the group will be selected to receive any given message.</para>
        /// <para>The <see cref="IJetStreamSubscription"/> returned will start delivering messages
        /// to the event handler as soon as they are received.</para>
        /// </remarks>
        /// <param name="subject">The subject on which to listen for messages.
        /// The subject can have wildcards (partial: <c>*</c>, full: <c>&gt;</c>).</param>
        /// <param name="queue">The name of the queue group in which to participate.</param>
        /// <param name="handler">The <see cref="EventHandler{MsgHandlerEventArgs}"/> invoked when messages are received 
        /// on the returned <see cref="IAsyncSubscription"/>.</param>
        /// <returns>An <see cref="IAsyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>.</returns>
        /// <seealso cref="ISubscription.Subject"/>
        /// <seealso cref="ISubscription.Queue"/>
        /// <param name="autoAck">Whether or not to auto ack the message</param>
        /// <param name="options">JetStream pull subscription options.</param>
        /// <returns>A JetStream push subscription</returns>
        IJetStreamPushAsyncSubscription PushSubscribeAsync(string subject, string queue, EventHandler<MsgHandlerEventArgs> handler, bool autoAck, PushSubscribeOptions options);

        /// <summary>
        /// Creates a synchronous JetStream subscriber on the given <paramref name="subject"/>.
        /// </summary>
        /// <param name="subject">The subject on which to listen for messages.</param>
        /// <returns>An <see cref="IJetStreamPushSyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>, as part of 
        /// the given queue group.</returns>
        /// <seealso cref="ISubscription.Subject"/>
        IJetStreamPushSyncSubscription PushSubscribeSync(string subject);

        /// <summary>
        /// Creates a synchronous JetStream subscriber on the given <paramref name="subject"/>.
        /// </summary>
        /// <param name="subject">The subject on which to listen for messages.</param>
        /// <param name="options">JetStream subscription options.</param>
        /// <returns>An <see cref="IJetStreamPushSyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>, as part of 
        /// the given queue group.</returns>
        /// <seealso cref="ISubscription.Subject"/>
        IJetStreamPushSyncSubscription PushSubscribeSync(string subject, PushSubscribeOptions options);

        /// <summary>
        /// Creates a synchronous JetStream queue subscriber on the given <paramref name="subject"/>.
        /// </summary>
        /// <remarks>All subscribers with the same queue name will form the queue group and
        /// only one member of the group will be selected to receive any given message
        /// synchronously.</remarks>
        /// <param name="subject">The subject on which to listen for messages.</param>
        /// <param name="queue">The name of the queue group in which to participate.</param>
        /// <returns>An <see cref="IJetStreamPushSyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>, as part of 
        /// the given queue group.</returns>
        /// <seealso cref="ISubscription.Subject"/>
        /// <seealso cref="ISubscription.Queue"/>
        IJetStreamPushSyncSubscription PushSubscribeSync(string subject, string queue);

        /// <summary>
        /// Creates a synchronous JetStream queue subscriber on the given <paramref name="subject"/>.
        /// </summary>
        /// <remarks>All subscribers with the same queue name will form the queue group and
        /// only one member of the group will be selected to receive any given message
        /// synchronously.</remarks>
        /// <param name="subject">The subject on which to listen for messages.</param>
        /// <param name="queue">The name of the queue group in which to participate.</param>
        /// <param name="options">JetStream subscription options.</param>
        /// <returns>An <see cref="IJetStreamPushSyncSubscription"/> to use to read any messages received
        /// from the NATS Server on the given <paramref name="subject"/>, as part of 
        /// the given queue group.</returns>
        /// <seealso cref="ISubscription.Subject"/>
        /// <seealso cref="ISubscription.Queue"/>
        IJetStreamPushSyncSubscription PushSubscribeSync(string subject, string queue, PushSubscribeOptions options);
    }
}
