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
using NATS.Client.JetStream;

namespace NATS.Client.KeyValue
{
    public class KeyValueWatchSubscription
    {
        private readonly IJetStreamPushAsyncSubscription sub;

        public KeyValueWatchSubscription(IJetStream js, string bucketName, string keyPattern, 
            bool headersOnly, Action<KeyValueEntry> watcher, params KeyValueOperation[] operations)
        {
            string stream = KeyValueUtil.StreamName(bucketName);
            string subject = KeyValueUtil.KeySubject(bucketName, keyPattern);
            
            PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                .WithStream(stream)
                // .WithOrdered(true) TODO WHEN ORDERED IS READY
                .WithConfiguration(
                    ConsumerConfiguration.Builder()
                        .WithAckPolicy(AckPolicy.None)
                        .WithDeliverPolicy(DeliverPolicy.LastPerSubject)
                        .WithHeadersOnly(headersOnly)
                        .WithFilterSubject(subject)
                        .Build())
                .Build();

            EventHandler<MsgHandlerEventArgs> handler;
            if (operations == null || operations.Length == 0)
            {
                handler = (sender, args) => watcher.Invoke(new KeyValueEntry(args.msg));
            }
            else
            {
                handler = (sender, args) =>
                {
                    foreach (KeyValueOperation op in operations)
                    {
                        KeyValueEntry kve = new KeyValueEntry(args.msg);
                        if (kve.Operation.Equals(op))
                        {
                            watcher.Invoke(kve);
                            return;
                        }
                    }
                };
            }

            sub = js.PushSubscribeAsync(subject, handler, false, pso);
        }

        public void Unsubscribe()
        {
            sub.Unsubscribe();
        }
    }
}