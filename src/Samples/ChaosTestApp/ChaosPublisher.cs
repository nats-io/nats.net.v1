using System;
using System.Threading;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATSExamples
{
    public class ChaosPublisher
    {
        const string PublisherLabel = "PUBLISHER";

        readonly ChaosCommandLine cmd;
        private readonly int pubDelay;
        private PublishAck lastPa; 
        readonly InterlockedLong errorRun = new InterlockedLong(0);

        public ChaosPublisher(ChaosCommandLine cmd, int pubDelay) {
            this.cmd = cmd;
            this.pubDelay = pubDelay;
        }

        public ulong LastSeqno()
        {
            return lastPa?.Seq ?? 0;
        }
        
        public bool IsInErrorState() {
            return errorRun.Read() > 0;
        }

        public void Run()
        {
            Random r = new Random();
            bool first = true;
            long started = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            using (IConnection conn = new ConnectionFactory().CreateConnection(cmd.MakeOptions(() => PublisherLabel)))
            {
                IJetStream js = conn.CreateJetStreamContext();
                while (true)
                {
                    if (first)
                    {
                        ChaosOutput.ControlMessage(PublisherLabel, "Starting Publish");
                        first = false;
                    }
                    
                    try
                    {
                        PublishAck pa = js.Publish(cmd.Subject, null);
                        Interlocked.Exchange(ref lastPa, pa);
                        if (errorRun.Read() > 0)
                        {
                            ChaosOutput.ControlMessage(PublisherLabel, "Restarting Publish");
                        }
                        errorRun.Set(0);
                    }
                    catch (Exception e)
                    {
                        if (errorRun.Increment() == 1)
                        {
                            ChaosOutput.ControlMessage(PublisherLabel, e.Message);
                        }
                    }

                    Thread.Sleep(r.Next(0, pubDelay));
                }
            }
        }
        
        private static String uptime(long started) {
            return Duration.OfMillis(DateTimeOffset.Now.ToUnixTimeMilliseconds() - started).ToDescription().Replace("DUR", "");
        }
    }
}