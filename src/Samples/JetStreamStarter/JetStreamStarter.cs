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
    using (IConnection c = new ConnectionFactory().CreateConnection("localhost"))
    {
        IJetStream js = c.CreateJetStreamContext();
        IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

        try { jsm.DeleteStream("strm"); } catch (NATSJetStreamException) {}

        StreamConfiguration streamConfiguration = new StreamConfiguration.StreamConfigurationBuilder()
            .WithStorageType(StorageType.Memory)
            .WithName("strm")
            .WithSubjects(
                "sub.*",
                "sub.segment")
            .Build();
        jsm.AddStream(streamConfiguration);

        js.Publish("sub.segment", Encoding.UTF8.GetBytes("msgs"));
        
        Console.WriteLine(jsm.GetStreamInfo("strm").ToString());
    }
}
    }
}
