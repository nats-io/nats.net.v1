// Copyright 2015-2018 The NATS Authors
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
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Linq.Expressions;
using System.Threading;
using NATS.Client;
using Xunit;

namespace UnitTests
{
    public class TestSubscriptions
    {
        static readonly string[] invalidSubjects = { "foo bar", "foo..bar", ".foo", "bar.baz.", "baz\t.foo" };
        static readonly string[] invalidQNames = { "foo group", "group\t1", "g1\r\n2" };

        [Fact]
        public void TestSubscriptionValidationAPI()
        {
            Assert.True(Subscription.IsValidSubject("foo"));
            Assert.True(Subscription.IsValidSubject("foo.bar"));

            foreach (string s in invalidSubjects)
            {
                Assert.False(Subscription.IsValidSubject(s));
            }

            foreach (string s in invalidQNames)
            {
                Assert.False(Subscription.IsValidQueueGroupName(s));
            }
        }

        [Fact]
        public void TestSlowConsumerDetectedEventWillGetFiredOnMessageCount()
        {
            int slowConsumerEventCalled = 0;
            var o = new Options
            {
                SlowConsumerEventHandler = (sender, args) => slowConsumerEventCalled++
            };
            var conn = new Connection(o);
            var subscription = new Subscription(conn, "subject", "queue");
            subscription.SetPendingLimits(100,1000);
            for (int i = 0; i < 110; i++)
            {
                var added = subscription.addMessage(new Msg("subject", Array.Empty<byte>()), o.SubChannelLength);
                Assert.Equal(i<100, added);
                Assert.Equal(i>=100, subscription.sc);
            }
            conn.Dispose();
            Assert.Equal(1, slowConsumerEventCalled);
            Assert.Equal(10, subscription.Dropped);
        }
        [Fact]
        public void TestSlowConsumerDetectedEventWillGetFiredOnMessageCountExceedingSubChannelLength()
        {
            int slowConsumerEventCalled = 0;
            var o = new Options
            {
                SubChannelLength = 100,
                SlowConsumerEventHandler = (sender, args) => slowConsumerEventCalled++
            };
            var conn = new Connection(o);
            var subscription = new AsyncSubscription(conn, "subject", "queue");
            for (int i = 0; i < 110; i++)
            {
                var added = subscription.addMessage(new Msg("subject", Array.Empty<byte>()), o.SubChannelLength);
                Assert.Equal(i < 100, added);
                Assert.Equal(i >= 100, subscription.sc);
            }
            conn.Dispose();
            Assert.Equal(1, slowConsumerEventCalled);
            Assert.Equal(10, subscription.Dropped);
        }
        [Fact]
        public void TestSlowConsumerDetectedEventWillGetFiredOnMessageSize()
        {
            int slowConsumerEventCalled = 0;
            var o = new Options
            {
                SlowConsumerEventHandler = (sender, args) => slowConsumerEventCalled++
            };
            var conn = new Connection(o);
            var subscription = new Subscription(conn, "subject", "queue");
            subscription.SetPendingLimits(10000, 1000);
            for (int i = 0; i < 110; i++)
            {
                var added = subscription.addMessage(new Msg("subject", new byte[10]), o.SubChannelLength);
                Assert.Equal(i < 100, added);
                Assert.Equal(i >= 100, subscription.sc);
            }
            conn.Dispose();
            Assert.Equal(1, slowConsumerEventCalled);
            Assert.Equal(10, subscription.Dropped);
        }
        [Fact]
        public void TestSlowConsumerWillBeResolvedAndRefiredOnSyncSubscription()
        {
            int slowConsumerEventCalled = 0;
            var o = new Options
            {
                SlowConsumerEventHandler = (sender, args) => slowConsumerEventCalled++
            };
            var conn = new Connection(o);
            var subscription = new SyncSubscription(conn, "subject", "queue");
            subscription.SetPendingLimits(100, 1000);
            for (int i = 0; i < 110; i++)
            {
                var added = subscription.addMessage(new Msg("subject", Array.Empty<byte>()), o.SubChannelLength);
                Assert.Equal(i < 100, added);
                Assert.Equal(i >= 100, subscription.sc);
            }

            Assert.Throws<NATSSlowConsumerException>(()=>subscription.NextMessage(1));
            for (int i = 0; i < 21; i++) //SC clears when the message count drops BELOW 80% of PendingMessageLimit. So we need to remove 21 messages.
            {
                Assert.NotNull(subscription.NextMessage(1));
            }
            Assert.False(subscription.sc);
            for (int i = 0; i < 79; i++)
            {
                Assert.NotNull(subscription.NextMessage(1));
            }
            Assert.Equal(0, subscription.PendingMessages);

            for (int i = 0; i < 110; i++)
            {
                var added = subscription.addMessage(new Msg("subject", Array.Empty<byte>()), o.SubChannelLength);
                Assert.Equal(i < 100, added);
                Assert.Equal(i >= 100, subscription.sc);
            }

            conn.Dispose();
            Assert.Equal(2, slowConsumerEventCalled);
            Assert.Equal(20, subscription.Dropped);
        }
        [Fact]
        public void TestSlowConsumerWillBeResolvedAndRefiredOnAsyncSubscription()
        {
            var cts = new CancellationTokenSource(1000); //Timeout for all blocking calls so that the test does not hang when something goes wrong.
            
            int slowConsumerEventCalled = 0;
            var o = new Options
            {
                SlowConsumerEventHandler = (sender, args) => slowConsumerEventCalled++
            };
            var conn = new Connection(o);
            var subscription = new AsyncSubscription(conn, "subject", "queue");
            subscription.SetPendingLimits(100, 1000);
            
            //For each item that is added to the handlerSteering collection the Message handler unblocks.
            //We can add a ManualResetEvent in this test case when we want to wait for the handler to have run up to this item.
            var handlerSteering = new System.Collections.Concurrent.BlockingCollection<ManualResetEventSlim>();
            subscription.MessageHandler += (sender, args) =>
            {
                Assert.NotNull(args.Message);
                handlerSteering.Take(cts.Token)?.Set();
            };
            conn.status = ConnState.CONNECTED;
            subscription.enableAsyncProcessing();
            for (int i = 0; i < 110; i++)
            {
                var added = subscription.addMessage(new Msg("subject",Array.Empty<byte>()){ sub = subscription }, o.SubChannelLength);
            }
            //The MessageHandler blocks at the first message. but that message is nevertheless removed from the buffer.
            //But as its async we don't know if its removed before or after the SC occurs.
            Assert.True(subscription.sc);


            var resetEvent = new ManualResetEventSlim(false);
            for (int i = 0; i < 20; i++)
            {
                handlerSteering.Add(null, cts.Token);
            }
            handlerSteering.Add(resetEvent, cts.Token);
            resetEvent.Wait(cts.Token);
            Assert.False(subscription.sc);

            resetEvent.Reset();

            var messagesToAdd = 110 - subscription.PendingMessages;
            for (int i = 0; i < messagesToAdd; i++)
            {
                var added = subscription.addMessage(new Msg("subject", Array.Empty<byte>()){sub = subscription}, o.SubChannelLength);
            }

            conn.Dispose();
            Assert.Equal(2, slowConsumerEventCalled);
            Assert.InRange(subscription.Dropped, 19, 20);
        }

    }
}