// Copyright 2022 The NATS Authors
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
using System.Collections.Generic;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;
using NATS.Client.KeyValue;

namespace NATSExamples.NatsByExample
{
    abstract class KvIntro
    {
        public static void KvIntroMain()
        {
            string natsUrl = Environment.GetEnvironmentVariable("NATS_URL");
            if (natsUrl == null)
            {
                natsUrl = "nats://127.0.0.1:4222";
            }

            // Create a new connection factory to create a connection.
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = natsUrl;

            // Creates a connection to nats server at the `natsUrl`
            // An `IConnection` is `IDisposable` so it can be use
            // within `using` statement. 
            ConnectionFactory cf = new ConnectionFactory();
            using (IConnection c = cf.CreateConnection(opts))
            {
                // ### Bucket basics
                // A key-value (KV) bucket is created by specifying a bucket name.
                // Java returns a KeyValueStatus object upon creation
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                KeyValueConfiguration kvc = KeyValueConfiguration.Builder()
                    .WithName("profiles")
                    .Build();

                KeyValueStatus keyValueStatus = kvm.Create(kvc);

                // Retrieve the Key Value context once the bucket is created.
                IKeyValue kv = c.CreateKeyValueContext("profiles");

                // As one would expect, the `KeyValue` interface provides the
                // standard `Put` and `Get` methods. However, unlike most KV
                // stores, a revision number of the entry is tracked.
                kv.Put("sue.color", Encoding.UTF8.GetBytes("blue"));
                KeyValueEntry entry = kv.Get("sue.color");
                Console.WriteLine("{0} {1} -> {2}", entry.Key, entry.Revision, entry.ValueAsString());

                kv.Put("sue.color", "green");
                entry = kv.Get("sue.color");
                Console.WriteLine("{0} {1} -> {2}", entry.Key, entry.Revision, entry.ValueAsString());

                // A revision number is useful when you need to enforce [optimistic
                // concurrency control][occ] on a specific key-value entry. In short,
                // if there are multiple actors attempting to put a new value for a
                // key concurrently, we want to prevent the "last writer wins" behavior
                // which is non-deterministic. To guard against this, we can use the
                // `kv.Update` method and specify the expected revision. Only if this
                // matches on the server, will the value be updated.
                // [occ]: https://en.wikipedia.org/wiki/Optimistic_concurrency_control
                try {
                    kv.Update("sue.color", Encoding.UTF8.GetBytes("red"), 1);
                }
                catch (NATSJetStreamException e) {
                    Console.WriteLine(e.Message);
                }

                ulong lastRevision = entry.Revision;
                kv.Update("sue.color", Encoding.UTF8.GetBytes("red"), lastRevision);
                entry = kv.Get("sue.color");
                Console.WriteLine("{0} {1} -> {2}", entry.Key, entry.Revision, entry.ValueAsString());

                // ### Stream abstraction
                // Before moving on, it is important to understand that a KV bucket is
                // light abstraction over a standard stream. This is by design since it
                // enables some powerful features which we will observe in a minute.
                //
                // **How exactly is a KV bucket modeled as a stream?**
                // When one is created, internally, a stream is created using the `KV_`
                // prefix as convention. Appropriate stream configuration are used that
                // are optimized for the KV access patterns, so you can ignore the
                // details.
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                IList<string> streamNames = jsm.GetStreamNames();
                Console.WriteLine(string.Join(",", streamNames));

                // Since it is a normal stream, we can create a consumer and
                // fetch messages.
                // If we look at the subject, we will notice that first token is a
                // special reserved prefix, the second token is the bucket name, and
                // remaining suffix is the actually key. The bucket name is inherently
                // a namespace for all keys and thus there is no concern for conflict
                // across buckets. This is different from what we need to do for a stream
                // which is to bind a set of _public_ subjects to a stream.
                IJetStream js = c.CreateJetStreamContext();

                PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                    .WithStream("KV_profiles").Build();
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(">", pso);

                Msg m = sub.NextMessage(100);
                Console.WriteLine("{0} {1} -> {2}", m.Subject, m.MetaData.StreamSequence, Encoding.UTF8.GetString(m.Data));
                
                // Let's put a new value for this key and see what we get from the subscription.
                kv.Put("sue.color", Encoding.UTF8.GetBytes("yellow"));
                m = sub.NextMessage(100);
                Console.WriteLine("{0} {1} -> {2}", m.Subject, m.MetaData.StreamSequence, Encoding.UTF8.GetString(m.Data));

                // Unsurprisingly, we get the new updated value as a message.
                // Since it's a KV interface, we should be able to delete a key as well.
                // Does this result in a new message?
                kv.Delete("sue.color");
                m = sub.NextMessage(100);
                Console.WriteLine("{0} {1} -> {2}", m.Subject, m.MetaData.StreamSequence, Encoding.UTF8.GetString(m.Data));

                // 🤔 That is useful to get a message that something happened to that key,
                // and that this is considered a new revision.
                // However, how do we know if the new value was set to be `nil` or the key
                // was deleted?
                // To differentiate, delete-based messages contain a header. Notice the `KV-Operation: DEL`
                // header.
                Console.WriteLine("Headers:");
                MsgHeader headers = m.Header;
                foreach (string key in headers.Keys)
                {
                    Console.WriteLine("  {0}:{1}", key, headers[key]);
                }
                
                // Notice that we can use a wildcard for watching keys.
                // See the implementation of `IntroKeyValueWatcher` down below.
                IntroKeyValueWatcher watcher = new IntroKeyValueWatcher();
                kv.Watch("sue.*", watcher, KeyValueWatchOption.UpdatesOnly);

                // Even though we deleted the key, of course we can put a new value.
                // In Java, there are a variety of `Put` signatures also, so here just put a string.
                kv.Put("sue.color", "purple");

                // To finish this short intro, since we know that keys are subjects under the covers, if we
                // put another key, we can observe the change through the watcher. One other detail to call out
                // is notice the revision for this *new* key is not `1`. It relies on the underlying stream's
                // message sequence number to indicate the _revision_. The guarantee being that it is always
                // monotonically increasing, but numbers will be shared across keys (like subjects) rather
                // than sequence numbers relative to each key.
                kv.Put("sue.food", "pizza");

                // Sleep this thread a little so the program has time
                // to receive all the messages before the program quits.
                Thread.Sleep(500);
            }
        }
        
        // ### Watching for changes
        // Although one could subscribe to the stream directly, it is more convenient
        // to use an `IKeyValueWatcher` which provides a deliberate API and types for tracking
        // changes over time. This implementation will be used later in the example.
        class IntroKeyValueWatcher : IKeyValueWatcher 
        {
            public void Watch(KeyValueEntry entry)
            {
                Console.WriteLine("Watcher: {0} {1} -> {2}", entry.Key, entry.Revision, entry.ValueAsString());
            }

            // The end of data signal can be useful to known when the watcher has _caught up_ with the current updates before
            // tracking the new ones.
            public void EndOfData()
            {
                Console.WriteLine("Watcher: Received End Of Data Signal");
            }
        }
    }
}