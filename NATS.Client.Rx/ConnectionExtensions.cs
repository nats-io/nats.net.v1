using System;

namespace NATS.Client
{
    public static class ConnectionExtensions
    {
        /// <summary>
        /// Returns an observable sequence that signals when a new message on <paramref name="subject"/> is received.
        /// </summary>
        /// <param name="connection">The connection to subscribe to.</param>
        /// <param name="subject">The subject on which to listen for messages.</param>
        /// <returns>An observable sequence that signals when a new message is received.</returns>
        /// <remarks>A subscription will only be created after subscribing to the observable</remarks>
        public static IObservable<MsgHandlerEventArgs> ToObservable(this IConnection connection, string subject)
        {
            return new MsgObservable<MsgHandlerEventArgs>(h => connection.SubscribeAsync(subject, h));
        }

        /// <summary>
        /// Returns an observable sequence that signals when a new message on <paramref name="subject"/> is received.
        /// </summary>
        /// <param name="connection">The connection to subscribe to.</param>
        /// <param name="subject">The subject on which to listen for messages.</param>
        /// <param name="queue">The name of the queue group in which to participate.</param>
        /// <returns>An observable sequence that signals when a new message is received.</returns>
        /// <remarks>A subscription will only be created after subscribing to the observable</remarks>
        public static IObservable<MsgHandlerEventArgs> ToObservable(this IConnection connection, string subject, string queue)
        {
            return new MsgObservable<MsgHandlerEventArgs>(h => connection.SubscribeAsync(subject, queue, h));
        }

        /// <summary>
        /// Returns an observable sequence that signals when a new message on <paramref name="subject"/> is received.
        /// </summary>
        /// <param name="connection">The connection to subscribe to.</param>
        /// <param name="subject">The subject on which to listen for messages.</param>
        /// <returns>An observable sequence that signals when a new message is received.</returns>
        /// <remarks>A subscription will only be created after subscribing to the observable</remarks>
        public static IObservable<EncodedMessageEventArgs> ToObservable(this IEncodedConnection connection, string subject)
        {
            return new MsgObservable<EncodedMessageEventArgs>(h => connection.SubscribeAsync(subject, h));
        }

        /// <summary>
        /// Returns an observable sequence that signals when a new message on <paramref name="subject"/> is received.
        /// </summary>
        /// <param name="connection">The connection to subscribe to.</param>
        /// <param name="subject">The subject on which to listen for messages.</param>
        /// <param name="queue">The name of the queue group in which to participate.</param>
        /// <returns>An observable sequence that signals when a new message is received.</returns>
        /// <remarks>A subscription will only be created after subscribing to the observable</remarks>
        public static IObservable<EncodedMessageEventArgs> ToObservable(this IEncodedConnection connection, string subject, string queue)
        {
            return new MsgObservable<EncodedMessageEventArgs>(h => connection.SubscribeAsync(subject, queue, h));
        }
    }
}