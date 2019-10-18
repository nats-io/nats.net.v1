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
    public static class OpsExtensions
    {
        /// <summary>
        /// Subscribes a delegating observer.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ob"></param>
        /// <param name="onNext"></param>
        /// <param name="onError"></param>
        /// <param name="onCompleted"></param>
        /// <returns></returns>
        public static IDisposable Subscribe<T>(this INATSObservable<T> ob, Action<T> onNext, Action<Exception> onError = null, Action onCompleted = null)
            => (ob ?? throw new ArgumentNullException(nameof(ob))).Subscribe(new DelegatingObserver<T>(onNext, onError, onCompleted));

        /// <summary>
        /// Subscribes a Safe delegating observer. Safe means that in the event of a failing observer, it will not get unsubscribed and disposed
        /// but instead still be seen as a valid observer.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ob"></param>
        /// <param name="onNext"></param>
        /// <param name="onError"></param>
        /// <param name="onCompleted"></param>
        /// <returns></returns>
        public static IDisposable SubscribeSafe<T>(this INATSObservable<T> ob, Action<T> onNext, Action<Exception> onError = null, Action onCompleted = null)
            => (ob ?? throw new ArgumentNullException(nameof(ob))).Subscribe(new SafeObserver<T>(onNext, onError, onCompleted));

        /// <summary>
        /// Subscribes a Safe observer. Safe means that in the event of a failing observer, it will not get unsubscribed and disposed
        /// but instead still be seen as a valid observer.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ob"></param>
        /// <param name="observer"></param>
        /// <returns></returns>
        public static IDisposable SubscribeSafe<T>(this INATSObservable<T> ob, IObserver<T> observer)
            => (ob ?? throw new ArgumentNullException(nameof(ob))).Subscribe(new SafeObserver<T>(observer.OnNext, observer.OnError, observer.OnCompleted));

        /// <summary>
        /// Applies passed predicate <paramref name="predicate"/> to filter the stream of <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ob"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static INATSObservable<T> Where<T>(this INATSObservable<T> ob, Func<T, bool> predicate)
            => new WhereObservable<T>(ob, predicate);

        /// <summary>
        /// Maps observable of <typeparamref name="TSrc"/> to observable of <typeparamref name="TResult"/>.
        /// </summary>
        /// <typeparam name="TSrc"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="ob"></param>
        /// <param name="mapper"></param>
        /// <returns></returns>
        public static INATSObservable<TResult> Select<TSrc, TResult>(this INATSObservable<TSrc> ob, Func<TSrc, TResult> mapper)
            => new MapObservable<TSrc,TResult>(ob, mapper);
    }
}