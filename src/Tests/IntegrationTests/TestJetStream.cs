// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Text;
using NATS.Client;
using NATS.Client.JetStream;
using Xunit;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

namespace IntegrationTests
{
    public class TestJetStream : TestSuite<JetStreamSuiteContext>
    {
        public TestJetStream(JetStreamSuiteContext context) : base(context) {}

        [Fact]
        public void TestJetStreamContextCreate()
        {
            Context.RunInJsServer(c =>
            {
                CreateTestStream(c); // tries management functions
                c.CreateJetStreamManagementContext().GetAccountStatistics(); // another management
                c.CreateJetStreamContext().Publish(SUBJECT, DataBytes(1));
            });
        }

        [Fact]
        public void TestJetStreamNotEnabled()
        {
            Context.RunInServer(c =>
            {
                Assert.Throws<NATSNoRespondersException>(
                    () => c.CreateJetStreamContext().PushSubscribeSync(SUBJECT));

                Assert.Throws<NATSNoRespondersException>(
                    () => c.CreateJetStreamManagementContext().GetAccountStatistics());
            });
        }

        [Fact]
        public void TestBindPush()
        {
            Context.RunInJsServer(c =>
            {
                CreateTestStream(c);
                IJetStream js = c.CreateJetStreamContext();

                JsPublish(js, SUBJECT, 1, 1);
                PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                    .WithDurable(DURABLE)
                    .Build();
                IJetStreamPushSyncSubscription s = js.PushSubscribeSync(SUBJECT, pso);
                Msg m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(1), Encoding.ASCII.GetString(m.Data));
                m.Ack();
                s.Unsubscribe();

                JsPublish(js, SUBJECT, 2, 1);
                pso = PushSubscribeOptions.Builder()
                    .WithStream(STREAM)
                    .WithDurable(DURABLE)
                    .Bind(true)
                    .Build();
                s = js.PushSubscribeSync(SUBJECT, pso);
                m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(2), Encoding.ASCII.GetString(m.Data));
                m.Ack();
                s.Unsubscribe();

                JsPublish(js, SUBJECT, 3, 1);
                pso = PushSubscribeOptions.BindTo(STREAM, DURABLE);
                s = js.PushSubscribeSync(SUBJECT, pso);
                m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(3), Encoding.ASCII.GetString(m.Data));

                Assert.Throws<ArgumentException>(
                () => PushSubscribeOptions.Builder().WithStream(STREAM).Bind(true).Build());

                Assert.Throws<ArgumentException>(
                () => PushSubscribeOptions.Builder().WithDurable(DURABLE).Bind(true).Build());

                Assert.Throws<ArgumentException>(
                () => PushSubscribeOptions.Builder().WithStream(String.Empty).Bind(true).Build());

                Assert.Throws<ArgumentException>(
                () => PushSubscribeOptions.Builder().WithStream(STREAM).WithDurable(String.Empty).Bind(true).Build());
            });
        }

        [Fact]
        public void TestBindPull()
        {
            Context.RunInJsServer(c =>
            {
                CreateTestStream(c);
                IJetStream js = c.CreateJetStreamContext();
                
                JsPublish(js, SUBJECT, 1, 1);

                PullSubscribeOptions pso = PullSubscribeOptions.Builder()
                    .WithDurable(DURABLE)
                    .Build();
                IJetStreamPullSubscription s = js.PullSubscribe(SUBJECT, pso);
                s.Pull(1);
                Msg m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(1), Encoding.ASCII.GetString(m.Data));
                m.Ack();
                s.Unsubscribe();

                JsPublish(js, SUBJECT, 2, 1);
                pso = PullSubscribeOptions.Builder()
                    .WithStream(STREAM)
                    .WithDurable(DURABLE)
                    .Bind(true)
                    .Build();
                s = js.PullSubscribe(SUBJECT, pso);
                s.Pull(1);
                m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(2), Encoding.ASCII.GetString(m.Data));
                m.Ack();
                s.Unsubscribe();

                JsPublish(js, SUBJECT, 3, 1);
                pso = PullSubscribeOptions.BindTo(STREAM, DURABLE);
                s = js.PullSubscribe(SUBJECT, pso);
                s.Pull(1);
                m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(3), Encoding.ASCII.GetString(m.Data));
            });
        }

        [Fact]
        public void TestBindErrors()
        {
            Context.RunInJsServer(c =>
            {
                CreateTestStream(c);

                IJetStream js = c.CreateJetStreamContext();
                
                PushSubscribeOptions pushso = PushSubscribeOptions.BindTo(STREAM, DURABLE);
                ArgumentException ae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, pushso));
                Assert.Contains("[SUB-B01]", ae.Message);
                
                PullSubscribeOptions pullso = PullSubscribeOptions.BindTo(STREAM, DURABLE);
                ae = Assert.Throws<ArgumentException>(() => js.PullSubscribe(SUBJECT, pullso));
                Assert.Contains("[SUB-B01]", ae.Message);
            });
        }

        [Fact]
        public void TestBindDurableDeliverSubject()
        {
            Context.RunInJsServer(c =>
            {
                CreateTestStream(c);

                IJetStream js = c.CreateJetStreamContext();
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                
            // create a durable push subscriber - has deliver subject
            ConsumerConfiguration ccDurPush = ConsumerConfiguration.Builder()
                    .WithDurable(Durable(1))
                    .WithDeliverSubject(Deliver(1))
                    .Build();
            jsm.AddOrUpdateConsumer(STREAM, ccDurPush);

            // create a durable pull subscriber - notice no deliver subject
            ConsumerConfiguration ccDurPull = ConsumerConfiguration.Builder()
                    .WithDurable(Durable(2))
                    .Build();
            jsm.AddOrUpdateConsumer(STREAM, ccDurPull);

            // try to pull subscribe against a push durable
            ArgumentException ae = Assert.Throws<ArgumentException>(
                    () => js.PullSubscribe(SUBJECT, PullSubscribeOptions.Builder().WithDurable(Durable(1)).Build())
            );
            Assert.Contains("[SUB-DS01]", ae.Message);

            // try to pull bind against a push durable
            ae = Assert.Throws<ArgumentException>(
                    () => js.PullSubscribe(SUBJECT, PullSubscribeOptions.BindTo(STREAM, Durable(1)))
            );
            Assert.Contains("[SUB-DS01]", ae.Message);

            // this one is okay
            IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, PullSubscribeOptions.Builder().WithDurable(Durable(2)).Build());
            sub.Unsubscribe(); // so I can re-use the durable

            // try to push subscribe against a pull durable
            ae = Assert.Throws<ArgumentException>(
                    () => js.PushSubscribeSync(SUBJECT, PushSubscribeOptions.Builder().WithDurable(Durable(2)).Build())
            );
            Assert.Contains("[SUB-DS02]", ae.Message);

            // try to push bind against a pull durable
            ae = Assert.Throws<ArgumentException>(
                    () => js.PushSubscribeSync(SUBJECT, PushSubscribeOptions.BindTo(STREAM, Durable(2)))
            );
            Assert.Contains("[SUB-DS02]", ae.Message);

            // try to push subscribe but mismatch the deliver subject
            ConsumerConfiguration ccMis = ConsumerConfiguration.Builder().WithDeliverSubject("not-match").Build();
            PushSubscribeOptions psoMis = PushSubscribeOptions.Builder().WithDurable(Durable(1))
                .WithConfiguration(ccMis).Build();
            ae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, psoMis));
            Assert.Contains("[SUB-DS03]", ae.Message);

            // this one is okay
            js.PushSubscribeSync(SUBJECT, PushSubscribeOptions.Builder().WithDurable(Durable(1)).Build());
                
            });
        }

        [Fact]
        public void TestGetConsumerInfoFromSubscription()
        {
            Context.RunInJsServer(c =>
            {
                CreateTestStream(c);

                IJetStream js = c.CreateJetStreamContext();

                IJetStreamPushSyncSubscription psync = js.PushSubscribeSync(SUBJECT);
                ConsumerInfo ci = psync.GetConsumerInformation();
                Assert.Equal(STREAM, ci.Stream);

                PullSubscribeOptions pso = PullSubscribeOptions.Builder().WithDurable(DURABLE).Build();
                IJetStreamPullSubscription pull = js.PullSubscribe(SUBJECT, pso);
                ci = pull.GetConsumerInformation();
                Assert.Equal(STREAM, ci.Stream);
            });
        }
    }
}
