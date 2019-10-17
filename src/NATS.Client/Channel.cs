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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
            SingleUseChannel<T> item;
            if (Channels.TryTake(out item))
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

    // This channel class, really a blocking queue, is named the way it is
    // so the code more closely reads with GO.  We implement our own channels 
    // to be lightweight and performant - other concurrent classes do the
    // task but are more heavyweight that what we want.
    internal sealed class Channel<T>
    {
        readonly Queue<T> q;
        readonly Object qLock = new Object();

        bool finished = false;

        public string Name { get; set; }

        internal Channel()
            : this(1024)
        {
        }

        internal Channel(int initialCapacity)
        {
            Name = "Unnamed channel " + this.GetHashCode();
            q = new Queue<T>(initialCapacity);
        }

        internal T get(int timeout)
        {
            var lockWasTaken = false;

            try
            {
                Monitor.Enter(qLock, ref lockWasTaken);

                if (finished)
                {
                    return default(T);
                }

                if (q.Count > 0)
                {
                    return q.Dequeue();
                }

                // if we had a *rare* spurious wakeup from Monitor.Wait(),
                // we could have an empty queue.  Protect this case by
                // rechecking in a loop.  This should be very rare, 
                // so keep it simple and just wait again.  This may result in 
                // a longer timeout that specified.
                do
                {
                    if (timeout < 0)
                    {
                        Monitor.Wait(qLock);
                    }
                    else
                    {
                        if (Monitor.Wait(qLock, timeout) == false)
                        {
                            throw new NATSTimeoutException();
                        }
                    }

                    // we waited, but are woken up by a finish...
                    if (finished)
                        return default(T);

                    // we can have an empty queue if there was a spurious wakeup.
                    if (q.Count > 0)
                    {
                        return q.Dequeue();
                    }
                } while (true);
            }
            finally
            {
                if (lockWasTaken)
                    Monitor.Exit(qLock);
            }
        } // get

        // Gets all available items in the queue, up to the size of the input buffer.
        // Returns the number of items delivered into the buffer.
        internal int get(int timeout, T[] buffer)
        {
            if (buffer.Length < 1) throw new ArgumentException();

            int delivered = 0;
            var lockWasTaken = false;

            try
            {
                Monitor.Enter(qLock, ref lockWasTaken);

                if (finished)
                {
                    return 0;
                }

                if (q.Count > 0)
                {
                    for (int ii = 0; ii < buffer.Length && q.Count > 0; ++ii)
                    {
                        buffer[ii] = q.Dequeue();
                        delivered++;
                    }

                    return delivered;
                }

                // if we had a *rare* spurious wakeup from Monitor.Wait(),
                // we could have an empty queue.  Protect this case by
                // rechecking in a loop.  This should be very rare, 
                // so keep it simple and just wait again.  This may result in 
                // a longer timeout that specified.
                do
                {
                    if (timeout < 0)
                    {
                        Monitor.Wait(qLock);
                    }
                    else
                    {
                        if (Monitor.Wait(qLock, timeout) == false)
                        {
                            throw new NATSTimeoutException();
                        }
                    }

                    // we waited, but are woken up by a finish...
                    if (finished)
                        return 0;

                    // we can have an empty queue if there was a spurious wakeup.
                    if (q.Count > 0)
                    {
                        for (int ii = 0; ii < buffer.Length && q.Count > 0; ++ii)
                        {
                            buffer[ii] = q.Dequeue();
                            delivered++;
                        }

                        return delivered;
                    }

                } while (true);
            }
            finally
            {
                if (lockWasTaken)
                    Monitor.Exit(qLock);
            }
        } // get

        internal void add(T item)
        {
            var lockWasTaken = false;

            try
            {
                Monitor.Enter(qLock, ref lockWasTaken);

                q.Enqueue(item);

                // if the queue count was previously zero, we were
                // waiting, so signal.
                if (q.Count <= 1)
                {
                    Monitor.Pulse(qLock);
                }
            }
            finally
            {
                if (lockWasTaken)
                    Monitor.Exit(qLock);
            }
        }

        // attempt to add an item, if-and-only-if the queue has not
        // exceeded the given upper bound.
        internal bool tryAdd(T item, int upperBound)
        {
            var lockWasTaken = false;

            try
            {
                Monitor.Enter(qLock, ref lockWasTaken);

                if (q.Count >= upperBound)
                    return false;

                q.Enqueue(item);

                // if the queue count was previously zero, we were
                // waiting, so signal.
                if (q.Count <= 1)
                {
                    Monitor.Pulse(qLock);
                }

                return true;
            }
            finally
            {
                if (lockWasTaken)
                    Monitor.Exit(qLock);
            }
        }

        internal void close()
        {
            var lockWasTaken = false;

            try
            {
                Monitor.Enter(qLock, ref lockWasTaken);

                finished = true;

                q.Clear();

                Monitor.Pulse(qLock);
            }
            finally
            {
                if (lockWasTaken)
                    Monitor.Exit(qLock);
            }
        }

        internal int Count
        {
            get
            {
                int rv;
                var lockWasTaken = false;
                try
                {
                    Monitor.Enter(qLock, ref lockWasTaken);
                    rv = q.Count;
                }
                finally
                {
                    if (lockWasTaken)
                        Monitor.Exit(qLock);
                }

                return rv;
            }
        }

    } // class Channel

}

