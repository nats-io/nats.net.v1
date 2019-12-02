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
    internal sealed class WhereObservable<T> : INATSObservable<T>
    {
        private readonly INATSObservable<T> src;
        private readonly Func<T, bool> predicate;

        public WhereObservable(INATSObservable<T> src, Func<T, bool> predicate)
        {
            this.src = src ?? throw new ArgumentNullException(nameof(src));
            this.predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
        }

        public void Dispose() => src?.Dispose();

        public IDisposable Subscribe(IObserver<T> observer)
            => src.SubscribeSafe(new WhereObserver(observer, predicate));

        private sealed class WhereObserver : IObserver<T>
        {
            private readonly IObserver<T> observer;
            private readonly Func<T, bool> predicate;

            public WhereObserver(IObserver<T> observer, Func<T, bool> predicate)
            {
                this.observer = observer ?? throw new ArgumentNullException(nameof(observer));
                this.predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            }

            public void OnNext(T value)
            {
                if (predicate(value))
                    observer.OnNext(value);
            }

            public void OnError(Exception error)
                => observer.OnError(error);

            public void OnCompleted()
                => observer.OnCompleted();
        }
    }
}