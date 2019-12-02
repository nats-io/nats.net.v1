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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NATS.Client;
using NATS.Client.Rx;
using NATS.Client.Rx.Ops;
using Xunit;

namespace IntegrationTests
{
    public class TestRx : TestSuite<RxSuiteContext>, IDisposable
    {
        private TestSync sync;

        public TestRx(RxSuiteContext context) : base(context) { }

        public void Dispose() => sync?.Dispose();

        [Fact]
        public void WhenPublishingAllSubscribedObserversShouldGetTheMessage()
        {
            sync = TestSync.FourActors();
            var subject = "132dc3101ef74ede90913c70baa542de";
            var payload = SamplePayload.Random();

            using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using (var cn = Context.OpenConnection(Context.Server1.Port))
                {
                    using (var observable = cn.Observe(subject))
                    {
                        var interceptingOb = new TestObserver(_ => sync.SignalComplete());
                        var interceptingSafeOb = new TestObserver(_ => sync.SignalComplete());
                        Msg delegatingObIntercept = null, safeDelegatingObIntercept = null;

                        observable.Subscribe(interceptingOb);
                        observable.SubscribeSafe(interceptingSafeOb);
                        observable.Subscribe(msg =>
                        {
                            delegatingObIntercept = msg;
                            sync.SignalComplete();
                        });
                        observable.SubscribeSafe(msg =>
                        {
                            safeDelegatingObIntercept = msg;
                            sync.SignalComplete();
                        });

                        cn.Publish(subject, payload);

                        sync.WaitForAll();
                        Assert.Equal(payload, interceptingOb.OnNextResults.Single());
                        Assert.Equal(payload, interceptingSafeOb.OnNextResults.Single());
                        Assert.Equal(payload, delegatingObIntercept);
                        Assert.Equal(payload, safeDelegatingObIntercept);
                    }
                }
            }
        }

        [Fact]
        public void WhenSubscribingMoreThanOneObserverToTheObservableOnlyOneServerSubscriptionShouldBeSetup()
        {
            sync = TestSync.FourActors();
            var subject = "eaa520002f1f42b0b5e2d0e162822780";
            var payload = SamplePayload.Random();

            using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using (var cn = Context.OpenConnection(Context.Server1.Port))
                {
                    using (var observable = cn.Observe(subject))
                    {
                        var interceptingOb = new TestObserver(_ => sync.SignalComplete());
                        var interceptingSafeOb = new TestObserver(_ => sync.SignalComplete());

                        observable.Subscribe(interceptingOb);
                        observable.Subscribe(interceptingSafeOb);
                        observable.Subscribe(_ => sync.SignalComplete());
                        observable.SubscribeSafe(_ => sync.SignalComplete());

                        cn.Publish(subject, payload);

                        sync.WaitForAll();
                        Assert.Equal(1, cn.SubscriptionCount);
                    }
                }
            }
        }

        [Fact]
        public void WhenObservingMoreThanOneSubjectTheyShouldNotInterfere()
        {
            sync = TestSync.TwoActors();

            var subject1 = "eaa520002f1f42b0b5e2d0e162822780";
            var subject2 = "319198581087493da52b48c96de2bf7e";
            var payload1 = SamplePayload.Random();
            var payload2 = SamplePayload.Random();

            using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using (var cn = Context.OpenConnection(Context.Server1.Port))
                {
                    using (var observable1 = cn.Observe(subject1))
                    using (var observable2 = cn.Observe(subject2))
                    {
                        var interceptingOb1 = new TestObserver(_ => sync.SignalComplete());
                        var interceptingOb2 = new TestObserver(_ => sync.SignalComplete());

                        observable1.Subscribe(interceptingOb1);
                        observable2.Subscribe(interceptingOb2);

                        cn.Publish(subject1, payload1);
                        cn.Publish(subject2, payload2);

                        sync.WaitForAll();
                        Assert.Equal(payload1, interceptingOb1.OnNextResults.Single());
                        Assert.Equal(payload2, interceptingOb2.OnNextResults.Single());
                    }
                }
            }
        }

        [Fact]
        public void WhenDisposingAnObserverItShouldNotReceiveMoreMessages()
        {
            sync = TestSync.TwoActors();

            var subject = "415b09873e3348d3a953e93d35ff61bf";
            var payload1 = SamplePayload.Random();
            var payload2 = SamplePayload.Random();

            using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                var interceptingOb1 = new TestObserver(_ => sync.SignalComplete());
                var interceptingOb2 = new TestObserver(_ => sync.SignalComplete());

                using (var cn = Context.OpenConnection(Context.Server1.Port))
                {
                    using (var observable = cn.Observe(subject))
                    {
                        using (observable.Subscribe(interceptingOb1))
                        {
                            using (observable.Subscribe(interceptingOb2))
                            {
                                cn.Publish(subject, payload1);
                                sync.WaitForAll();
                            }

                            cn.Publish(subject, payload2);
                            sync.WaitForOne();
                        }

                        Assert.Equal(new[] { payload1.Data, payload2.Data }, interceptingOb1.OnNextResults.Select(i => i.Data).ToArray());
                        Assert.Equal(new[] { payload1.Data }, interceptingOb2.OnNextResults.Select(i => i.Data).ToArray());
                    }
                }
            }
        }

        [Fact]
        public void WhenDisposingAnObservableNoMoreDispatchesShouldBeDone()
        {
            sync = TestSync.TwoActors();

            var subject = "bea00c947f4c470cb4e884c58202d3ac";
            var payload1 = SamplePayload.Random();
            var payload2 = SamplePayload.Random();

            using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                var interceptingOb1 = new TestObserver(_ => sync.SignalComplete());
                var interceptingOb2 = new TestObserver(_ => sync.SignalComplete());

                using (var cn = Context.OpenConnection(Context.Server1.Port))
                {
                    var observable1 = cn.Observe(subject);
                    var observable2 = cn.Observe(subject);

                    observable1.Subscribe(interceptingOb1);
                    observable2.Subscribe(interceptingOb2);

                    cn.Publish(subject, payload1);
                    sync.WaitForAll();

                    observable1.Dispose();

                    cn.Publish(subject, payload2);
                    sync.WaitForOne();

                    observable2.Dispose();

                    Assert.Equal(new[] { payload1.Data }, interceptingOb1.OnNextResults.Select(i => i.Data).ToArray());
                    Assert.Equal(new[] { payload1.Data, payload2.Data }, interceptingOb2.OnNextResults.Select(i => i.Data).ToArray());
                }
            }
        }

        private class TestObserver : IObserver<Msg>
        {
            private readonly Action<Msg> onNext;

            private int onCompletedCount;
            private readonly ConcurrentQueue<Exception> onErrorResults = new ConcurrentQueue<Exception>();
            private readonly ConcurrentQueue<Msg> onNextResults = new ConcurrentQueue<Msg>();

            public int OnCompletedCount => onCompletedCount;
            public IEnumerable<Msg> OnNextResults => onNextResults;
            public IEnumerable<Exception> OnErrorResults => onErrorResults;

            public TestObserver(Action<Msg> onNext = null)
            {
                this.onNext = onNext;
            }

            public void OnCompleted() => Interlocked.Increment(ref onCompletedCount);
            public void OnNext(Msg value)
            {
                onNextResults.Enqueue(value);
                onNext?.Invoke(value);
            }

            public void OnError(Exception error) => onErrorResults.Enqueue(error);
        }
    }
}