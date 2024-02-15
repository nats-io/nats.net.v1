using System;
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    public class SimpleFetchConsumer : ConnectableConsumer
    {
        readonly IStreamContext sc;
        readonly IConsumerContext cc;
        readonly int batchSize;
        readonly int expiresIn;
        readonly Thread t;

        IFetchConsumer fc;
        
        public SimpleFetchConsumer(CommandLine cmd, ConsumerKind consumerKind, int batchSize, int expiresIn) : base(cmd, "fc", consumerKind)
        {
            if (consumerKind == ConsumerKind.Ordered) {
                throw new ArgumentException("Ordered Consumer not supported for App Simple Fetch");
            }

            this.batchSize = batchSize;
            this.expiresIn = expiresIn;

            sc = Conn.GetStreamContext(cmd.Stream);

            cc = sc.CreateOrUpdateConsumer(NewCreateConsumer().Build());
            Output.ControlMessage(Label, cc.ConsumerName);
            t = new Thread(() => run());
            t.Start();
        }

        public void run()
        {
            FetchConsumeOptions fco = FetchConsumeOptions.Builder().WithMaxMessages(batchSize).WithExpiresIn(expiresIn).Build();
            Output.ControlMessage(Label, ToString(fco));
            
            while (true)
            {
                try
                {
                    using (IFetchConsumer disposableFc = cc.Fetch(fco))
                    {
                        fc = disposableFc;
                        Msg m = fc.NextMessage();
                        while (m != null)
                        {
                            OnMessage(m);
                            m = fc.NextMessage();
                        }
                    }
                }
                catch (Exception)
                {
                    // if there was an error, just try again
                }

                // simulating some work to be done between fetches
                Output.WorkMessage(Label, "Fetch Batch Completed, Last Received Seq: " + lastReceivedSequence.Read());
                Thread.Sleep(10);
            }
        }

        public override void refreshInfo()
        {
            if (fc != null) {
                UpdateLabel(cc.ConsumerName);
            }
        }
        
        
        public static String ToString(FetchConsumeOptions fco) {
            return "FetchConsumeOptions" +
                   "\nMax Messages: " + fco.MaxMessages +
                   "\nMax Bytes: " + fco.MaxBytes +
                   "\nExpires In: " + fco.ExpiresInMillis +
                   "\nIdleHeartbeat: " + fco.IdleHeartbeat +
                   "\nThreshold Percent: " + fco.ThresholdPercent;
        }

    }
}