using NATS.Client;
using NATS.Client.JetStream;
using static UnitTests.TestBase;

namespace IntegrationTests
{
    public static class JetStreamTestUtils
    {
        public static void CreateTestStream(IConnection c)
            => CreateMemoryStream(c, STREAM, SUBJECT);

        public static void CreateMemoryStream(IConnection c, string streamName, params string[] subjects)
        {
            var jsm = c.CreateJetStreamManagementContext();
            var sc = StreamConfiguration.Builder()
                .WithName(streamName)
                .WithStorageType(StorageType.Memory)
                .WithSubjects(subjects)
                .Build();
            jsm.AddStream(sc);
        }
    }
}