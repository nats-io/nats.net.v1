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

namespace NATS.Client.KeyValue
{
    public class KeyValueWatchSubscription : IDisposable
    {
        private IJetStreamPushAsyncSubscription sub;
        private readonly InterlockedBoolean endOfDataSent;
        private readonly object subLock;

        public KeyValueWatchSubscription(KeyValue kv, string keyPattern,
            IKeyValueWatcher watcher, params KeyValueWatchOption[] watchOptions)
        {
            subLock = new object();
            string subscribeSubject = kv.ReadSubject(keyPattern);
            
            // figure out the result options
            bool headersOnly = false;
            bool includeDeletes = true;
            DeliverPolicy deliverPolicy = DeliverPolicy.LastPerSubject;
            foreach (KeyValueWatchOption wo in watchOptions)
            {
                switch (wo) 
                {
                    case KeyValueWatchOption.MetaOnly: headersOnly = true; break;
                    case KeyValueWatchOption.IgnoreDelete: includeDeletes = false; break;
                    case KeyValueWatchOption.UpdatesOnly: deliverPolicy = DeliverPolicy.New; break;
                    case KeyValueWatchOption.IncludeHistory: deliverPolicy = DeliverPolicy.All; break;
                }
            }

            if (deliverPolicy == DeliverPolicy.New || kv._getLast(subscribeSubject) == null) 
            {
                endOfDataSent = new InterlockedBoolean(true);
                watcher.EndOfData();
            }
            else
            {
                endOfDataSent = new InterlockedBoolean(false);
            }
            
            PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                .WithStream(kv.StreamName)
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
                KeyValueEntry kve = new KeyValueEntry(args.Message);
                if (includeDeletes || kve.Operation.Equals(KeyValueOperation.Put))
                {
                    watcher.Watch(kve);
                }

                if (endOfDataSent.IsFalse() && kve.Delta == 0)
                {
                    endOfDataSent.Set(true);
                    watcher.EndOfData();
                }
            };

            sub = kv.js.PushSubscribeAsync(subscribeSubject, handler, false, pso);
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
            lock (subLock)
            {
                if (sub != null)
                {
                    try
                    {
                        sub?.Unsubscribe();
                    }
                    catch (Exception)
                    {
                        // ignore all exceptions, nothing we can really do.
                    }
                    finally
                    {
                        sub = null;
                    }
                }
            }
        }

        public void Dispose()
        {
            Unsubscribe();
        }
    }
}