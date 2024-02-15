using System;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATSExamples
{
    public abstract class ChaosConnectableConsumer
    {
        protected readonly IConnection Conn;
        protected readonly IJetStream Js;
        public InterlockedLong lastReceivedSequence;
        public ulong LastReceivedSequence => (ulong)lastReceivedSequence.Read(); 
        protected readonly ChaosConsumerKind ConsumerKind;
        protected readonly EventHandler<MsgHandlerEventArgs> Handler;

        protected readonly ChaosCommandLine Cmd;
        protected string Initials;
        public string Name { get; protected set; }
        public string DurableName { get; protected set; }
        public string Label { get; protected set; }

        public ChaosConnectableConsumer(ChaosCommandLine cmd, string initials, ChaosConsumerKind consumerKind)
        {
            Cmd = cmd;
            lastReceivedSequence = new InterlockedLong(0);
            ConsumerKind = consumerKind;
            
            switch (consumerKind) {
                case ChaosConsumerKind.Durable:
                    DurableName = initials + "-dur-" + ConsumerName();
                    Name = DurableName;
                    break;
                case ChaosConsumerKind.Ephemeral:
                    DurableName = null;
                    Name = initials + "-eph-" + ConsumerName();
                    break;
                case ChaosConsumerKind.Ordered:
                    DurableName = null;
                    Name = initials + "-ord-" + ConsumerName();
                    break;
            }
            Initials = initials;
            Label = Name + " (" + ChaosEnums.ToString(ConsumerKind) + ")"; 

            Conn = new ConnectionFactory().CreateConnection(cmd.MakeOptions(() => Label));
            Js = Conn.CreateJetStreamContext();

            Handler = (s, e) =>
            {
                Msg m = e.Message;
                OnMessage(m);
            };
        }

        private static string ConsumerName()
        {
            return "" + new Nuid().GetNextSequence();
        }

        protected void OnMessage(Msg m)
        {
            m.Ack();
            long seq = (long)m.MetaData.StreamSequence;
            long lastSeq = lastReceivedSequence.Read();
            lastReceivedSequence.Set(seq);
            ChaosOutput.WorkMessage(Label, "Last Received Seq: " + seq + "(" + lastSeq + ")");
        }

        public abstract void refreshInfo();

        protected void UpdateLabel(string conName) {
            if (!Name.Contains(conName))
            {
                int at = Name.LastIndexOf("-");
                Name = Name.Substring(0, at + 1) + conName;
                Label = Name + " (" + ChaosEnums.ToString(ConsumerKind) + ")";
            }
        }
 
        protected ConsumerConfiguration.ConsumerConfigurationBuilder NewCreateConsumer() {
            return recreateConsumer(0);
        }

        protected ConsumerConfiguration.ConsumerConfigurationBuilder recreateConsumer(ulong last) {
            return ConsumerConfiguration.Builder()
                .WithName(ConsumerKind == ChaosConsumerKind.Ordered ? null : Name)
                .WithDurable(DurableName)
                .WithDeliverPolicy(last == 0 ? DeliverPolicy.All : DeliverPolicy.ByStartSequence)
                .WithStartSequence(last == 0 ? 0 : last + 1)
                .WithFilterSubject(Cmd.Subject);
        }
    }
}