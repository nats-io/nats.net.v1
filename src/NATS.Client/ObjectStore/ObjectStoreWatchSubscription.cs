// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATS.Client.ObjectStore
{
    public class ObjectStoreWatchSubscription : IDisposable
    {
        private IJetStreamPushAsyncSubscription sub;
        private readonly InterlockedBoolean endOfDataSent;

        public ObjectStoreWatchSubscription(ObjectStore os,
            IObjectStoreWatcher watcher, params ObjectStoreWatchOption[] watchOptions)
        {
            string subscribeSubject = os.RawAllMetaSubject();
            
            // figure out the result options
            bool headersOnly = false;
            bool includeDeletes = true;
            DeliverPolicy deliverPolicy = DeliverPolicy.LastPerSubject;
            foreach (ObjectStoreWatchOption wo in watchOptions) {
                switch (wo) {
                    case ObjectStoreWatchOption.IgnoreDelete: includeDeletes = false; break;
                    case ObjectStoreWatchOption.UpdatesOnly: deliverPolicy = DeliverPolicy.New; break;
                    case ObjectStoreWatchOption.IncludeHistory: deliverPolicy = DeliverPolicy.All; break;
                }
            }

            if (deliverPolicy == DeliverPolicy.New
                || os._getLast(subscribeSubject) == null) {
                endOfDataSent = new InterlockedBoolean(true);
                watcher.EndOfData();
            }
            else
            {
                endOfDataSent = new InterlockedBoolean(false);
            }
            
            PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                .WithStream(os.StreamName)
                .WithOrdered(true)
                .WithConfiguration(
                    ConsumerConfiguration.Builder()
                        .WithAckPolicy(AckPolicy.None)
                        .WithDeliverPolicy(deliverPolicy)
                        .WithHeadersOnly(headersOnly)
                        .WithFilterSubject(subscribeSubject)
                        .Build())
                .Build();

            EventHandler<MsgHandlerEventArgs> handler = (sender, args) =>
            {
                ObjectInfo oi = new ObjectInfo(args.Message);
                if (includeDeletes || !oi.IsDeleted)
                {
                    watcher.Watch(oi);
                }

                if (endOfDataSent.IsFalse() && args.Message.MetaData.NumPending == 0)
                {
                    endOfDataSent.Set(true);
                    watcher.EndOfData();
                }
            };

            sub = os.js.PushSubscribeAsync(subscribeSubject, handler, false, pso);
            if (endOfDataSent.IsFalse())
            {
                ulong pending = sub.GetConsumerInformation().CalculatedPending;
                if (pending == 0)
                {
                    endOfDataSent.Set(true);
                    watcher.EndOfData();
                }
            }
        }

        public void Unsubscribe()
        {
            try
            {
                sub?.Unsubscribe();
            }
            finally
            {
                sub = null;
            }
        }

        public void Dispose()
        {
            Unsubscribe();
        }
    }
}
