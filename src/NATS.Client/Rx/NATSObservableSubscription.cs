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
    /// <summary>
    /// Represents an observable async subscription to which you
    /// can subscribe an consume messages from in push-style form.
    /// </summary>
    public sealed class NATSObservableSubscription : NATSObservable<Msg>, IDisposable
    {
        private readonly IAsyncSubscription subscription;

        private NATSObservableSubscription(IAsyncSubscription subscription)
        {
            this.subscription = subscription ?? throw new ArgumentNullException(nameof(subscription));

            this.subscription.MessageHandler += OnIncomingMessage;
            this.subscription.Start();
        }

        /// <summary>
        /// Wraps sent subscription and turns it into an observable.
        /// </summary>
        /// <param name="subscription"></param>
        /// <returns></returns>
        public static INATSObservable<Msg> Wrap(IAsyncSubscription subscription)
            => new NATSObservableSubscription(subscription); 

        private void OnIncomingMessage(object _, MsgHandlerEventArgs e)
            => InvokeObservers(e.Message);

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if(!disposing)
                return;

            subscription.MessageHandler -= OnIncomingMessage;
            subscription.Dispose();
        }
    }
}