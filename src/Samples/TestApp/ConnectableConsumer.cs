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
        protected String Initials;
        public String Name { get; protected set; }
        public String DurableName { get; protected set; }
        public String Label { get; protected set; }

        public ConnectableConsumer(CommandLine cmd, String initials, ConsumerKind consumerKind)
        {
            Cmd = cmd;
            lastReceivedSequence = new InterlockedLong(0);
            ConsumerKind = consumerKind;
            
            switch (consumerKind) {
                case ConsumerKind.Durable:
                    DurableName = initials + "-dur-" + new Nuid().GetNextSequence();
                    Name = DurableName;
                    break;
                case ConsumerKind.Ephemeral:
                    DurableName = null;
                    Name = initials + "-eph-" + new Nuid().GetNextSequence();
                    break;
                case ConsumerKind.Ordered:
                    DurableName = null;
                    Name = initials + "-ord-" + new Nuid().GetNextSequence();
                    break;
            }
            Initials = initials;
            UpdateNameAndLabel(Name);

            Conn = new ConnectionFactory().CreateConnection(cmd.MakeOptions(Label));
            Js = Conn.CreateJetStreamContext();

            Handler = (s, e) =>
            {
                Msg m = e.Message;
                m.Ack();
                long seq = (long)m.MetaData.StreamSequence;
                lastReceivedSequence.Set(seq);
                Output.Work(Label, "Last Received Seq: " + seq);
            };
        }
 
        public abstract void refreshInfo();

        protected void UpdateNameAndLabel(String updatedName) {
            Name = updatedName;
            if (updatedName == null)
            {
                Label = Enums.ToString(ConsumerKind);
            }
            else {
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