// Copyright 2015-2018 The NATS Authors
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

using System.Collections.Concurrent;
using System.Threading;

namespace NATS.Client
{
    // Provides a Channel<T> implementation that only allows a single call to 'get'
    // Used for ping-pong
    // Instance methods are not thread safe, however, this class is not intended
    // to be shared by any more than a single producer and single consumer.
    internal sealed class SingleUseChannel<T>
    {
        static readonly ConcurrentBag<SingleUseChannel<T>> Channels
            = new ConcurrentBag<SingleUseChannel<T>>();

        readonly ManualResetEventSlim e = new ManualResetEventSlim();
        volatile bool hasValue = false;
        T actualValue;

        // Get an existing unused SingleUseChannel from the pool,
        // or create one if none are available.
        // Thread safe.
        public static SingleUseChannel<T> GetOrCreate()
        {
            if (Channels.TryTake(out var item))
            {
                return item;
            }

            return new SingleUseChannel<T>();
        }

        // Return a SingleUseChannel to the internal pool.
        // Thread safe.
        public static void Return(SingleUseChannel<T> ch)
        {
            ch.reset();
            if (Channels.Count < 1024) Channels.Add(ch);
        }

        internal T get(int timeout)
        {
            while (!hasValue)
            {
                if (timeout < 0)
                {
                    e.Wait();
                }
                else
                {
                    if (!e.Wait(timeout))
                        throw new NATSTimeoutException();
                }
            }

            return actualValue;
        }

        internal void add(T value)
        {
            actualValue = value;
            hasValue = true;
            e.Set();
        }

        internal void reset()
        {
            hasValue = false;
            actualValue = default(T);
            e.Reset();
        }
    }
}
