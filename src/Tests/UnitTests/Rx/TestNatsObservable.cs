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
using NATS.Client.Rx;
using NATS.Client.Rx.Ops;
using Xunit;

namespace UnitTests.Rx
{
    public class TestNatsObservable
    {
        private readonly EmitableObservable<Data> unitUnderTest = new EmitableObservable<Data>();

        [Fact]
        public void WhenOkEmitSubscribersShouldAllGetTheSameMessage()
        {
            Data interceptedMessage = null;
            var interceptingOb = new TestObserver<Data>();
            var msg = new Data(42);

            unitUnderTest.Subscribe(i => interceptedMessage = i);
            unitUnderTest.Subscribe(interceptingOb);

            unitUnderTest.Emit(msg);

            Assert.Equal(msg, interceptedMessage);
            Assert.Equal(msg, interceptingOb.OnNextResults.Single());
        }

        [Fact]
        public void WhenOkEmitSafeSubscribersShouldAllGetTheSameMessage()
        {
            Data interceptedMessage = null;
            var interceptingOb = new TestObserver<Data>();
            var msg = new Data(42);

            unitUnderTest.SubscribeSafe(i => interceptedMessage = i);
            unitUnderTest.SubscribeSafe(interceptingOb);

            unitUnderTest.Emit(msg);

            Assert.Equal(msg, interceptedMessage);
            Assert.Equal(msg, interceptingOb.OnNextResults.Single());
        }

        [Fact]
        public void WhenOkEmitSubscribersShouldNotGetOnErrorCalled()
        {
            Exception interceptedEx = null;
            var interceptingOb = new TestObserver<Data>();
            var msg = new Data(42);

            unitUnderTest.Subscribe(_ => { }, ex => interceptedEx = ex);
            unitUnderTest.Subscribe(interceptingOb);

            unitUnderTest.Emit(msg);

            Assert.Null(interceptedEx);
            Assert.Empty(interceptingOb.OnErrorResults);
        }

        [Fact]
        public void WhenOkEmitSafeSubscribersShouldNotGetOnErrorCalled()
        {
            Exception interceptedEx = null;
            var interceptingOb = new TestObserver<Data>();
            var msg = new Data(42);

            unitUnderTest.SubscribeSafe(_ => { }, ex => interceptedEx = ex);
            unitUnderTest.SubscribeSafe(interceptingOb);

            unitUnderTest.Emit(msg);

            Assert.Null(interceptedEx);
            Assert.Empty(interceptingOb.OnErrorResults);
        }

        [Fact]
        public void WhenOkEmitSubscribersShouldNotGetOnCompletedCalled()
        {
            var interceptedOnCompleted = false;
            var interceptingOb = new TestObserver<Data>();
            var msg = new Data(42);

            unitUnderTest.Subscribe(_ => { }, onCompleted: () => interceptedOnCompleted = true);
            unitUnderTest.Subscribe(interceptingOb);

            unitUnderTest.Emit(msg);

            Assert.False(interceptedOnCompleted);
            Assert.Equal(0, interceptingOb.OnCompletedCount);
        }

        [Fact]
        public void WhenOkEmitSafeSubscribersShouldNotGetOnCompletedCalled()
        {
            var interceptedOnCompleted = false;
            var interceptingOb = new TestObserver<Data>();
            var msg = new Data(42);

            unitUnderTest.SubscribeSafe(_ => { }, onCompleted: () => interceptedOnCompleted = true);
            unitUnderTest.SubscribeSafe(interceptingOb);

            unitUnderTest.Emit(msg);

            Assert.False(interceptedOnCompleted);
            Assert.Equal(0, interceptingOb.OnCompletedCount);
        }

        [Fact]
        public void WhenDisposingAnOkObserverOnCompletedShouldBeCalled()
        {
            var interceptedOnCompleted = false;
            var interceptingOb = new TestObserver<Data>();
            var msg = new Data(42);

            using (unitUnderTest.Subscribe(_ => { }, _ => { }, () => interceptedOnCompleted = true))
            using (unitUnderTest.Subscribe(interceptingOb))
                unitUnderTest.Emit(msg);

            Assert.True(interceptedOnCompleted);
            Assert.Equal(1, interceptingOb.OnCompletedCount);
        }

        [Fact]
        public void WhenDisposingAnOkSafeObserverOnCompletedShouldBeCalled()
        {
            var interceptedOnCompleted = false;
            var interceptingOb = new TestObserver<Data>();
            var msg = new Data(42);

            using (unitUnderTest.SubscribeSafe(_ => { }, _ => { }, () => interceptedOnCompleted = true))
            using (unitUnderTest.SubscribeSafe(interceptingOb))
                unitUnderTest.Emit(msg);

            Assert.True(interceptedOnCompleted);
            Assert.Equal(1, interceptingOb.OnCompletedCount);
        }

        [Fact]
        public void WhenDisposingAnOkObserverOnErrorShouldNotBeCalled()
        {
            var interceptedOnError = false;
            var interceptingOb = new TestObserver<Data>();
            var msg = new Data(42);

            using (unitUnderTest.Subscribe(_ => { }, _ => interceptedOnError = true))
            using (unitUnderTest.Subscribe(interceptingOb))
                unitUnderTest.Emit(msg);

            Assert.False(interceptedOnError);
            Assert.Empty(interceptingOb.OnErrorResults);
        }

        [Fact]
        public void WhenDisposingAnOkSafeObserverOnErrorShouldNotBeCalled()
        {
            var interceptedOnError = false;
            var interceptingOb = new TestObserver<Data>();
            var msg = new Data(42);

            using (unitUnderTest.SubscribeSafe(_ => { }, _ => interceptedOnError = true))
            using (unitUnderTest.SubscribeSafe(interceptingOb))
                unitUnderTest.Emit(msg);

            Assert.False(interceptedOnError);
            Assert.Empty(interceptingOb.OnErrorResults);
        }

        [Fact]
        public void WhenDisposingAFailedObserverItShouldNotGetOnCompletedInvoked()
        {
            bool interceptedOnCompleted = false;
            var interceptingOb = new TestObserver<Data>(_ => throw new Exception("FAIL"));
            var org = new Data(42);

            using (unitUnderTest.Subscribe(_ => throw new Exception("FAIL"), onCompleted: () => interceptedOnCompleted = true))
            using (unitUnderTest.Subscribe(interceptingOb))
                unitUnderTest.Emit(org);

            Assert.False(interceptedOnCompleted);
            Assert.Equal(0, interceptingOb.OnCompletedCount);
        }

        [Fact]
        public void WhenDisposingAFailedSafeObserverItShouldGetOnCompletedInvoked()
        {
            bool interceptedOnCompleted = false;
            var interceptingOb = new TestObserver<Data>(_ => throw new Exception("FAIL"));
            var org = new Data(42);

            using (unitUnderTest.SubscribeSafe(_ => throw new Exception("FAIL"), onCompleted: () => interceptedOnCompleted = true))
            using (unitUnderTest.SubscribeSafe(interceptingOb))
                unitUnderTest.Emit(org);

            Assert.True(interceptedOnCompleted);
            Assert.Equal(1, interceptingOb.OnCompletedCount);
        }

        [Fact]
        public void WhenDispatchingToADisposedOkObserverItShallGetNoDispatch()
        {
            var interceptedMessages = new Queue<Data>();
            var interceptingOb = new TestObserver<Data>();
            var msg1 = new Data(42);
            var msg2 = new Data(43);

            using (unitUnderTest.Subscribe(interceptedMessages.Enqueue))
            using (unitUnderTest.Subscribe(interceptingOb))
                unitUnderTest.Emit(msg1);

            unitUnderTest.Emit(msg2);

            Assert.Single(interceptedMessages);
            Assert.Single(interceptingOb.OnNextResults);
        }

        [Fact]
        public void WhenDispatchingToADisposedOkSafeObserverItShallGetNoDispatch()
        {
            var interceptedMessages = new Queue<Data>();
            var interceptingOb = new TestObserver<Data>();
            var msg1 = new Data(42);
            var msg2 = new Data(43);

            using (unitUnderTest.SubscribeSafe(interceptedMessages.Enqueue))
            using (unitUnderTest.SubscribeSafe(interceptingOb))
                unitUnderTest.Emit(msg1);

            unitUnderTest.Emit(msg2);

            Assert.Single(interceptedMessages);
            Assert.Single(interceptingOb.OnNextResults);
        }

        [Fact]
        public void WhenOneObserversIsFailingItShouldNotGetOnErrorCalledAndOthersShouldBehaveNormally()
        {
            int interceptedOnNext1 = 0, interceptedOnError1 = 0, interceptedOnCompleted1 = 0;
            int interceptedOnNext2 = 0, interceptedOnError2 = 0, interceptedOnCompleted2 = 0;
            var interceptingOb = new TestObserver<Data>();
            var org = new Data(42);

            var s1 = unitUnderTest.Subscribe(
                _ => interceptedOnNext1 += 1,
                _ => interceptedOnError1 += 1,
                () => interceptedOnCompleted1 += 1);
            var s2 = unitUnderTest.Subscribe(
                _ => { interceptedOnNext2 += 1; throw new Exception("FAIL"); },
                _ => interceptedOnError2 += 1,
                () => interceptedOnCompleted2 += 1);
            var s3 = unitUnderTest.Subscribe(interceptingOb);

            unitUnderTest.Emit(org);
            unitUnderTest.Emit(org);

            Assert.Equal(2, interceptedOnNext1);
            Assert.Equal(0, interceptedOnError1);
            Assert.Equal(0, interceptedOnCompleted1);

            Assert.Equal(1, interceptedOnNext2);
            Assert.Equal(0, interceptedOnError2);
            Assert.Equal(0, interceptedOnCompleted2);

            Assert.Equal(2, interceptingOb.OnNextResults.Count());
            Assert.Empty(interceptingOb.OnErrorResults);
            Assert.Equal(0, interceptingOb.OnCompletedCount);

            s1.Dispose();
            s2.Dispose();
            s3.Dispose();
        }

        [Fact]
        public void WhenOneSafeObserversIsFailingItShouldNotGetOnErrorCalledAndOthersShouldBehaveNormally()
        {
            int interceptedOnNext1 = 0, interceptedOnError1 = 0, interceptedOnCompleted1 = 0;
            int interceptedOnNext2 = 0, interceptedOnError2 = 0, interceptedOnCompleted2 = 0;
            var interceptingOb = new TestObserver<Data>();
            var org = new Data(42);

            var s1 = unitUnderTest.SubscribeSafe(
                _ => interceptedOnNext1 += 1,
                _ => interceptedOnError1 += 1,
                () => interceptedOnCompleted1 += 1);
            var s2 = unitUnderTest.SubscribeSafe(
                _ => { interceptedOnNext2 += 1; throw new Exception("FAIL"); },
                _ => interceptedOnError2 += 1,
                () => interceptedOnCompleted2 += 1);
            var s3 = unitUnderTest.SubscribeSafe(interceptingOb);

            unitUnderTest.Emit(org);
            unitUnderTest.Emit(org);

            Assert.Equal(2, interceptedOnNext1);
            Assert.Equal(0, interceptedOnError1);
            Assert.Equal(0, interceptedOnCompleted1);

            Assert.Equal(2, interceptedOnNext2);
            Assert.Equal(0, interceptedOnError2);
            Assert.Equal(0, interceptedOnCompleted2);

            Assert.Equal(2, interceptingOb.OnNextResults.Count());
            Assert.Empty(interceptingOb.OnErrorResults);
            Assert.Equal(0, interceptingOb.OnCompletedCount);

            s1.Dispose();
            s2.Dispose();
            s3.Dispose();
        }

        [Fact]
        public void WhenObserverHasFailedItShallGetNoFurtherDispatches()
        {
            int interceptedMsgCount = 0;
            var interceptingOb = new TestObserver<Data>(_ => throw new Exception("FAIL"));
            var org = new Data(42);

            unitUnderTest.Subscribe(_ => { interceptedMsgCount += 1; throw new Exception("FAIL"); });
            unitUnderTest.Subscribe(interceptingOb);

            unitUnderTest.Emit(org);
            unitUnderTest.Emit(org);

            Assert.Equal(1, interceptedMsgCount);
            Assert.Single(interceptingOb.OnNextResults);
        }

        [Fact]
        public void WhenSafeObserverHasFailedItShouldGetFurtherDispatches()
        {
            int interceptedMsgCount = 0;
            var interceptingOb = new TestObserver<Data>(_ => throw new Exception("FAIL"));
            var org = new Data(42);

            unitUnderTest.SubscribeSafe(_ => { interceptedMsgCount += 1; throw new Exception("FAIL"); });
            unitUnderTest.SubscribeSafe(interceptingOb);

            unitUnderTest.Emit(org);
            unitUnderTest.Emit(org);

            Assert.Equal(2, interceptedMsgCount);
            Assert.Equal(2, interceptingOb.OnNextResults.Count());
        }

        [Fact]
        public void WhenUsingWhereObservablePredicateShouldBeHonored()
        {
            var oddOb = new TestObserver<Data>();
            var evenOb = new TestObserver<Data>();

            unitUnderTest.Where(i => i.Value % 2 != 0).Subscribe(oddOb);
            unitUnderTest.Where(i => i.Value % 2 == 0).Subscribe(evenOb);

            unitUnderTest.Emit(new Data(1));
            unitUnderTest.Emit(new Data(2));
            unitUnderTest.Emit(new Data(3));
            unitUnderTest.Emit(new Data(4));

            Assert.Equal(new[] { 1, 3 }, oddOb.OnNextResults.Select(i => i.Value));
            Assert.Equal(new[] { 2, 4 }, evenOb.OnNextResults.Select(i => i.Value));
        }

        [Fact]
        public void WhenUsingSelectObservableItShouldMap()
        {
            var ob = new TestObserver<int>();

            unitUnderTest.Select(i => i.Value).Subscribe(ob);

            unitUnderTest.Emit(new Data(1));
            unitUnderTest.Emit(new Data(2));
            unitUnderTest.Emit(new Data(3));
            unitUnderTest.Emit(new Data(4));

            Assert.Equal(new[] { 1, 2, 3, 4 }, ob.OnNextResults);
        }

        private class TestObserver<T> : IObserver<T>
        {
            private readonly Action<T> onNext;

            private int onCompletedCount;
            private readonly ConcurrentQueue<Exception> onErrorResults = new ConcurrentQueue<Exception>();
            private readonly ConcurrentQueue<T> onNextResults = new ConcurrentQueue<T>();

            public int OnCompletedCount => onCompletedCount;
            public IEnumerable<T> OnNextResults => onNextResults;
            public IEnumerable<Exception> OnErrorResults => onErrorResults;

            public TestObserver(Action<T> onNext = null)
            {
                this.onNext = onNext;
            }

            public void OnCompleted() => Interlocked.Increment(ref onCompletedCount);
            public void OnNext(T value)
            {
                onNextResults.Enqueue(value);
                onNext?.Invoke(value);
            }

            public void OnError(Exception error) => onErrorResults.Enqueue(error);
        }

        private class EmitableObservable<T> : NATSObservable<T>
        {
            public void Emit(T data) => InvokeObservers(data);
        }

        private class Data
        {
            public int Value { get; }

            public Data(int value)
            {
                Value = value;
            }
        }
    }
}