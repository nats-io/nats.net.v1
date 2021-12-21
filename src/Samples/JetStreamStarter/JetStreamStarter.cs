using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    /// <summary>
    /// Just some starter code to write your own example.
    /// </summary>
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

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                IJetStream js = c.CreateJetStreamContext();
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
            }
        }
    }
}