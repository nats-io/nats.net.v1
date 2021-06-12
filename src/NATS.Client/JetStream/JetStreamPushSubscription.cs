using System;
namespace NATS.Client.JetStream
{
    public class JetStreamPushSubscription : JetStreamSubscription, IJetStreamPushSubscription
    {

        internal event EventHandler<MsgHandlerEventArgs> MessageHandler;

        public JetStreamPushSubscription(JetStream js, Connection conn,
            string subject, string queue, EventHandler<MsgHandlerEventArgs> handler,
            ConsumerConfiguration cc,
            PushSubscribeOptions opts, bool autoAck) : base(js, conn, subject, queue, cc, opts)
        {
            if (autoAck)
            {
                MessageHandler = (obj, args) =>
                {
                    handler(obj, args);
                    args.Message.Ack();
                };
            }
            else
            {
                MessageHandler = handler;
            }
        }
    }
}
