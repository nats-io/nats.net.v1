using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    internal static class JetStreamStarter
    {
        static void Main(string[] args)
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            if (args.Length == 1)
            {
                opts.Url = args[0];
            }
            else
            {
                opts.Url = "nats://localhost:4222";
            }

            using (IConnection conn = new ConnectionFactory().CreateConnection(opts))
            {
                IJetStream js = conn.CreateJetStreamContext();
                IJetStreamManagement jsm = conn.CreateJetStreamManagementContext();
            }
        }
    }
}
