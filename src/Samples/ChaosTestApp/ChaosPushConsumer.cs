using NATS.Client.JetStream;

namespace NATSExamples
{
    public class ChaosPushConsumer : ChaosConnectableConsumer
    {
        readonly IJetStreamPushAsyncSubscription sub;
        
        public ChaosPushConsumer(ChaosCommandLine cmd, ChaosConsumerKind consumerKind) : base(cmd, "pu", consumerKind)
        {
            PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                .WithStream(cmd.Stream)
                .WithConfiguration(NewCreateConsumer()
                    .WithIdleHeartbeat(1000)
                    .Build())
                .WithOrdered(consumerKind == ChaosConsumerKind.Ordered)
                .Build();

            sub = Js.PushSubscribeAsync(cmd.Subject, Handler, false, pso);
        }

        public override void refreshInfo()
        {
            UpdateLabel(sub.Consumer);
        }
    }
}