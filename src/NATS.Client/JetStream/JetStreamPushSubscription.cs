using System;
namespace NATS.Client.JetStream
{
    public class JetStreamPushSubscription : JetStreamSubscription, IJetStreamPushSubscription
    {

        internal event EventHandler<MsgHandlerEventArgs> MessageHandler;

        public JetStreamPushSubscription(JetStream js, Connection conn,
            string subject, string queue, ConsumerConfiguration cc,
            SubscribeOptions opts) : base(js, conn, subject, queue, cc, opts)
        {
        }
    }
}
