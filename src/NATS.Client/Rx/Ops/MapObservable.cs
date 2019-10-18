// Copyright 2019 The NATS Authors
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

namespace NATS.Client.Rx.Ops
{
    internal sealed class MapObservable<TSrc, TResult> : INATSObservable<TResult>
    {
        private readonly INATSObservable<TSrc> src;
        private readonly Func<TSrc, TResult> mapper;

        public MapObservable(INATSObservable<TSrc> src, Func<TSrc, TResult> predicate)
        {
            this.src = src ?? throw new ArgumentNullException(nameof(src));
            this.mapper = predicate ?? throw new ArgumentNullException(nameof(predicate));
        }

        public void Dispose() => src?.Dispose();

        public IDisposable Subscribe(IObserver<TResult> observer)
            => src.SubscribeSafe(new MapObserver(observer, mapper));

        private sealed class MapObserver : IObserver<TSrc>
        {
            private readonly IObserver<TResult> observer;
            private readonly Func<TSrc, TResult> mapper;

            public MapObserver(IObserver<TResult> observer, Func<TSrc, TResult> mapper)
            {
                this.observer = observer ?? throw new ArgumentNullException(nameof(observer));
                this.mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            }

            public void OnNext(TSrc value) => observer.OnNext(mapper(value));

            public void OnError(Exception error)
                => observer.OnError(error);

            public void OnCompleted()
                => observer.OnCompleted();
        }
    }
}