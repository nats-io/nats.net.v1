using System.Collections.Generic;
using System.Text;
using NATS.Client;
using NATS.Client.JetStream;
using Xunit;
using static UnitTests.TestBase;

namespace IntegrationTests
{
    public static class JetStreamTestBase
    {
        public const string JsReplyTo = "$JS.ACK.test-stream.test-consumer.1.2.3.1605139610113260000";
        public static readonly int DefaultTimeout = 500; // millis

        public static void CreateDefaultTestStream(IConnection c)
            => CreateMemoryStream(c, STREAM, SUBJECT);

        public static void CreateDefaultTestStream(IJetStreamManagement jsm)
            => CreateMemoryStream(jsm, STREAM, SUBJECT);

        public static void CreateMemoryStream(IConnection c, string streamName, params string[] subjects)
        {
            var jsm = c.CreateJetStreamManagementContext();
            CreateMemoryStream(jsm, streamName, subjects);
        }

        public static void CreateMemoryStream(IJetStreamManagement jsm, string streamName, params string[] subjects)
        {
            try
            {
                jsm.DeleteStream(streamName); // since the server is re-used, we want a fresh stream
            }
            catch (NATSJetStreamException)
            {
                // it's might not have existed
            }

            jsm.AddStream(StreamConfiguration.Builder()
                .WithName(streamName)
                .WithStorageType(StorageType.Memory)
                .WithSubjects(subjects)
                .Build()
            );
        }

        // ----------------------------------------------------------------------------------------------------
        // Publish / Read
        // ----------------------------------------------------------------------------------------------------
        public static void JsPublish(IJetStream js, string subject, string prefix, int count) {
            for (int x = 1; x <= count; x++) {
                string data = prefix + x;
                js.Publish(new Msg(subject, Encoding.ASCII.GetBytes(data)));
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

        public static PublishAck JsPublish(IJetStream js) {
            return js.Publish(new Msg(SUBJECT, DataBytes()));
        }

        public static IList<Msg> ReadMessagesAck(ISyncSubscription sub)
        {
            IList<Msg> messages = new List<Msg>();
            try
            {
                Msg msg = sub.NextMessage(DefaultTimeout);
                while (msg != null) {
                    messages.Add(msg);
                    if (msg.IsJetStream) {
                        msg.Ack();
                    }
                    msg = sub.NextMessage(DefaultTimeout);
                }
            }
            catch (NATSTimeoutException)
            {
                // it's fine, just end
            }

            return messages;
        }

        public static void AssertNoMoreMessages(ISyncSubscription sub)
        {
            Assert.Empty(ReadMessagesAck(sub));
        } 
        
        public static void AckAll(IList<Msg> messages)
        {
            foreach (Msg m in messages)
            {
                m.Ack();                        
            }
        }

        // ----------------------------------------------------------------------------------------------------
        // Validate / Assert
        // ----------------------------------------------------------------------------------------------------
        public static void ValidateRedAndTotal(int expectedRed, int actualRed, int expectedTotal, int actualTotal) {
            ValidateRead(expectedRed, actualRed);
            ValidateTotal(expectedTotal, actualTotal);
        }

        public static void ValidateTotal(int expectedTotal, int actualTotal) {
            Assert.Equal(expectedTotal, actualTotal);
        }

        public static void ValidateRead(int expectedRed, int actualRed) {
            Assert.Equal(expectedRed, actualRed);
        }

        public static void AssertSubscription(IJetStreamSubscription sub, string stream, string consumer, string deliver, bool isPullMode) {
            Assert.Equal(stream, sub.Stream);
            if (consumer == null)
            {
                Assert.NotNull(sub.Consumer);
            }
            else
            {
                Assert.Equal(consumer, sub.Consumer);
            }

            if (deliver != null) {
                Assert.Equal(deliver, sub.DeliverSubject);
            }
            Assert.Equal(isPullMode, sub.IsPullMode());
        }

        public static void AssertSameMessages(IList<Msg> l1, IList<Msg> l2) {
            Assert.Equal(l1.Count, l2.Count);
            for (int x = 0; x < l1.Count; x++)
            {
                string data1 = Encoding.ASCII.GetString(l1[x].Data);
                string data2 = Encoding.ASCII.GetString(l2[x].Data);
                Assert.Equal(data1, data2);
            }
        }

        public static void AssertAllJetStream(IList<Msg> messages) {
            foreach (Msg m in messages) {
                AssertIsJetStream(m);
            }
        }

        public static void AssertIsJetStream(Msg m) {
            Assert.True(m.IsJetStream);
            Assert.False(m.HasStatus);
        }

        public static void AssertLastIsStatus(IList<Msg> messages, int code) {
            int lastIndex = messages.Count - 1;
            for (int x = 0; x < lastIndex; x++) {
                Msg m = messages[x];
                Assert.True(m.IsJetStream);
            }
            AssertIsStatus(messages[lastIndex], code);
        }

        public static void AssertStarts408(IList<Msg> messages, int count408, int expectedJs) {
            for (int x = 0; x < count408; x++) {
                AssertIsStatus(messages[x], 408);
            }
            int countedJs = 0;
            int lastIndex = messages.Count - 1;
            for (int x = count408; x <= lastIndex; x++) {
                Msg m = messages[x];
                Assert.True(m.IsJetStream);
                countedJs++;
            }
            Assert.Equal(expectedJs, countedJs);
        }

        private static void AssertIsStatus(Msg statusMsg, int code) {
            Assert.False(statusMsg.IsJetStream);
            Assert.True(statusMsg.HasStatus);
            Assert.Equal(code, statusMsg.Status.Code);
        }
    }
}