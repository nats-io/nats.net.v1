using NATS.Client.JetStream;

namespace NATSExamples
{
    public class SimpleConsumer : ConnectableConsumer
    {
        readonly IStreamContext sc;
        readonly IConsumerContext cc;
        readonly IOrderedConsumerContext occ;
        readonly IMessageConsumer mc;
        
        public SimpleConsumer(CommandLine cmd, ConsumerKind consumerKind, int batchSize, int expiresIn) : base(cmd, "sc", consumerKind)
        {
            sc = Conn.GetStreamContext(cmd.Stream);

            ConsumeOptions co = ConsumeOptions.Builder()
                .WithBatchSize(batchSize)
                .WithExpiresIn(expiresIn)
                .Build();

            if (consumerKind == ConsumerKind.Ordered) {
                OrderedConsumerConfiguration ocConfig = new OrderedConsumerConfiguration().WithFilterSubjects(cmd.Subject);
                cc = null;
                occ = sc.CreateOrderedConsumer(ocConfig);
                mc = occ.Consume(Handler, co);
            }
            else {
                occ = null;
                cc = sc.CreateOrUpdateConsumer(NewCreateConsumer().Build());
                mc = cc.Consume(Handler, co);
            }
            Output.ControlMessage(Label, mc.GetConsumerName());
        }

        public override void refreshInfo()
        {
            UpdateLabel(mc.GetConsumerName());
        }
    }
}