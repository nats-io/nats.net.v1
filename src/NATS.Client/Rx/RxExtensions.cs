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

namespace NATS.Client.Rx
{
    public static class RxExtensions
    {
        /// <summary>
        /// Subscribes to the passed subject and returns a hot observable. Hence unless you
        /// subscribe to the observable, no message will be handled and old messages will
        /// not be delivered, only new messages.
        /// </summary>
        /// <param name="cn">Connection to observe.</param>
        /// <param name="subject">Subject to observe.</param>
        /// <returns>Observable stream of messages.</returns>
        public static INATSObservable<Msg> Observe(this IConnection cn, string subject)
            => (cn ?? throw new ArgumentNullException(nameof(cn))).SubscribeAsync(subject).ToObservable();

        /// <summary>
        /// Turns the passed <see cref="IAsyncSubscription"/> to a hot observable. Hence unless you
        /// subscribe to the observable, no message will be handled and old messages will
        /// not be delivered, only new messages.
        /// </summary>
        /// <param name="subscription">Subscription to observe.</param>
        /// <returns>Observable stream of messages.</returns>
        /// <remarks>The passed subscription will be disposed when you dispose the observable.</remarks>
        public static INATSObservable<Msg> ToObservable(this IAsyncSubscription subscription)
            => NATSObservableSubscription.Wrap(subscription);
    }
}