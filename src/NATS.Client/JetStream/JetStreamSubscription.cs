using System;
namespace NATS.Client.JetStream
{
    public class JetStreamSubscription : Subscription, IJetStreamSubscription
    {
        public JetStream JS { get; }
        public ConsumerInfo ConsumerInformation { get; }


        internal ConsumerConfiguration ConsumerConfig { get; }
        internal SubscribeOptions SubscriptionOptions { get; }

        internal JetStreamSubscription(JetStream js, Connection conn,
            string subject, string queue, ConsumerConfiguration cc,
            SubscribeOptions opts) : base(conn, subject, queue)
        {
            JS = js;
            ConsumerConfig = cc;
            SubscriptionOptions = opts;
        }
    }
}
