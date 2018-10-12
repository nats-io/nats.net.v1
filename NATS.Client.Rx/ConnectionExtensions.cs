using System;

namespace NATS.Client
{
    public static class ConnectionExtensions
    {
        public static IObservable<MsgHandlerEventArgs> ToObservable(this IConnection connection, string subject)
        {
            return new MsgObservable<MsgHandlerEventArgs>(h => connection.SubscribeAsync(subject, h));
        }

        public static IObservable<MsgHandlerEventArgs> ToObservable(this IConnection connection, string subject, string queue)
        {
            return new MsgObservable<MsgHandlerEventArgs>(h => connection.SubscribeAsync(subject, queue, h));
        }
        
        public static IObservable<EncodedMessageEventArgs> ToObservable(this IEncodedConnection connection, string subject)
        {
            return new MsgObservable<EncodedMessageEventArgs>(h => connection.SubscribeAsync(subject, h));
        }

        public static IObservable<EncodedMessageEventArgs> ToObservable(this IEncodedConnection connection, string subject, string queue)
        {
            return new MsgObservable<EncodedMessageEventArgs>(h => connection.SubscribeAsync(subject, queue, h));
        }
    }
}