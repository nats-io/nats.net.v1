using NATS.Client.JetStream;

namespace NATSExamples
{
    public class PushConsumer : ConnectableConsumer
    {
        readonly IJetStreamPushAsyncSubscription sub;
        
        public PushConsumer(CommandLine cmd, ConsumerKind consumerKind) : base(cmd, "pu", consumerKind)
        {
            PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                .WithStream(cmd.Stream)
                .WithConfiguration(NewCreateConsumer()
                    .WithIdleHeartbeat(1000)
                    .Build())
                .WithOrdered(consumerKind == ConsumerKind.Ordered)
                .Build();

            sub = Js.PushSubscribeAsync(cmd.Subject, Handler, false, pso);
        }

        public override void refreshInfo()
        {
            UpdateNameAndLabel(sub.Consumer);
        }
    }
}