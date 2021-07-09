using System.Text;
using NATS.Client;
using NATS.Client.JetStream;
using static UnitTests.TestBase;

namespace IntegrationTests
{
    public static class JetStreamTestBase
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
        // ----------------------------------------------------------------------------------------------------
        // Publish / Read
        // ----------------------------------------------------------------------------------------------------
        public static void JsPublish(IJetStream js, string subject, string prefix, int count) {
            for (int x = 1; x <= count; x++) {
                string data = prefix + x;
                js.Publish(new Msg(SUBJECT, Encoding.ASCII.GetBytes(data)));
            }
        }

        public static void JsPublish(IJetStream js, string subject, int startId, int count) {
            for (int x = 0; x < count; x++) {
                js.Publish(new Msg(subject, DataBytes(startId++)));
            }
        }

        public static void JsPublish(IJetStream js, string subject, int count) {
            JsPublish(js, subject, 1, count);
        }

        public static void JsPublish(IConnection c, string subject, int count) {
            JsPublish(c.CreateJetStreamContext(), subject, 1, count);
        }

        public static void JsPublish(IConnection c, string subject, int startId, int count) {
            JsPublish(c.CreateJetStreamContext(), subject, startId, count);
        }

        public static PublishAck JsPublish(JetStream js) {
            return js.Publish(new Msg(SUBJECT, DataBytes()));
        }
    }
}