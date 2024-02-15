using System;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATSExamples
{
    public abstract class ConnectableConsumer
    {
        protected readonly IConnection Conn;
        protected readonly IJetStream Js;
        public InterlockedLong lastReceivedSequence;
        public ulong LastReceivedSequence => (ulong)lastReceivedSequence.Read(); 
        protected readonly ConsumerKind ConsumerKind;
        protected readonly EventHandler<MsgHandlerEventArgs> Handler;

        protected readonly CommandLine Cmd;
        protected string Initials;
        public string Name { get; protected set; }
        public string DurableName { get; protected set; }
        public string Label { get; protected set; }

        public ConnectableConsumer(CommandLine cmd, string initials, ConsumerKind consumerKind)
        {
            Cmd = cmd;
            lastReceivedSequence = new InterlockedLong(0);
            ConsumerKind = consumerKind;
            
            switch (consumerKind) {
                case ConsumerKind.Durable:
                    DurableName = initials + "-dur-" + ConsumerName();
                    Name = DurableName;
                    break;
                case ConsumerKind.Ephemeral:
                    DurableName = null;
                    Name = initials + "-eph-" + ConsumerName();
                    break;
                case ConsumerKind.Ordered:
                    DurableName = null;
                    Name = initials + "-ord-" + ConsumerName();
                    break;
            }
            Initials = initials;
            Label = Name + " (" + Enums.ToString(ConsumerKind) + ")"; 

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
            Output.WorkMessage(Label, "Last Received Seq: " + seq + "(" + lastSeq + ")");
        }

        public abstract void refreshInfo();

        protected void UpdateLabel(string conName) {
            if (!Name.Contains(conName))
            {
                int at = Name.LastIndexOf("-");
                Name = Name.Substring(0, at + 1) + conName;
                Label = Name + " (" + Enums.ToString(ConsumerKind) + ")";
            }
        }
 
        protected ConsumerConfiguration.ConsumerConfigurationBuilder NewCreateConsumer() {
            return recreateConsumer(0);
        }

        protected ConsumerConfiguration.ConsumerConfigurationBuilder recreateConsumer(ulong last) {
            return ConsumerConfiguration.Builder()
                .WithName(ConsumerKind == ConsumerKind.Ordered ? null : Name)
                .WithDurable(DurableName)
                .WithDeliverPolicy(last == 0 ? DeliverPolicy.All : DeliverPolicy.ByStartSequence)
                .WithStartSequence(last == 0 ? 0 : last + 1)
                .WithFilterSubject(Cmd.Subject);
        }
    }
}