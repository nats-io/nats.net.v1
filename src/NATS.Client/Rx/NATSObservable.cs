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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace NATS.Client.Rx
{
    /// <summary>
    /// Base-class for basing observable solutions upon.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class NATSObservable<T> : INATSObservable<T>
    {
        private readonly ConcurrentDictionary<int, ObserverSubscription> subscriptions = new ConcurrentDictionary<int, ObserverSubscription>();

        protected void InvokeObservers(T data)
        {
            foreach (var sub in subscriptions.Values)
            {
                try
                {
                    sub.Observer.OnNext(data);
                }
                catch
                {
                    //Misbehaving observer will remove its subscription
                    //and dispose it. No invoke of OnError as it's not the
                    //producer that is failing.
                    subscriptions.TryRemove(sub.Id, out _);

                    try
                    {
                        sub.Dispose();
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }
        }

        private void DisposeSubscription(ObserverSubscription sub)
        {
            if(!subscriptions.TryRemove(sub.Id, out _))
                return;

            try
            {
                sub.Observer.OnCompleted();
            }
            catch
            {
                // ignored
            }
        }

        /// <summary>
        /// Subscribes sent observer to the observable stream.
        /// </summary>
        /// <param name="observer">The Observer to invoke when messages arrive.</param>
        /// <returns>Subscription. Dispose when done consuming.</returns>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
                throw new ArgumentNullException(nameof(observer));

            var sub = new ObserverSubscription(observer, DisposeSubscription);
            
            if(!subscriptions.TryAdd(sub.Id, sub))
                throw new ArgumentException("Could not subscribe observer. Ensure it has not already been subscribed.", nameof(observer));

            return sub;
        }

        private sealed class ObserverSubscription : IDisposable
        {
            private readonly Action<ObserverSubscription> disposer;

            public readonly int Id;
            public readonly IObserver<T> Observer;

            public ObserverSubscription(IObserver<T> observer, Action<ObserverSubscription> disposer)
            {
                Id = observer.GetHashCode();
                Observer = observer;
                this.disposer = disposer;
            }

            public void Dispose() => disposer(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if(!disposing)
                return;

            var exceptions = new List<Exception>();

            foreach (var obSub in subscriptions)
            {
                try
                {
                    subscriptions.TryRemove(obSub.Key, out _);
                    obSub.Value.Dispose();
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }

            if(exceptions.Any())
                throw new NATSRxException(
                    "One or more exception(s) occurred while disposing the NATS Observable's' subscriptions. See inner exception (AggregateException) for more details.",
                    new AggregateException(exceptions));
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}