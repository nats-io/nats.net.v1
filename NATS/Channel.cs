// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NATS.Client
{
    // This channel class really a blocking queue, is named the way it is
    // so the code more closely reads with GO.  We implement our own channels 
    // to be lightweight and performant - other concurrent classes do the
    // task but are more heavyweight that what we want.
    internal sealed class Channel<T>
    {
        readonly Queue<T> q;
        readonly Object   qLock = new Object();

        bool   finished = false;
        
        internal Channel()
        {
            q = new Queue<T>(1024);
        }

        internal Channel(int initialCapacity)
        {
            q = new Queue<T>(initialCapacity);
        }

        internal T get(int timeout)
        {
            T rv = default(T);

            Monitor.Enter(qLock);

            if (finished)
            {
                Monitor.Exit(qLock);
                return default(T);
            }

            if (q.Count > 0)
            {
                rv = q.Dequeue();
                Monitor.Exit(qLock);
                return rv;
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
                        // Unlock before exiting.
                        Monitor.Exit(qLock);
                        throw new NATSTimeoutException();
                    }
                }

                // we waited, but are woken up by a finish...
                if (finished)
                    break;

                // we can have an empty queue if there was a spurious wakeup.
                if (q.Count > 0)
                {
                    rv = q.Dequeue();
                    break;
                }

            } while (true);

            Monitor.Exit(qLock);

            return rv;

        } // get
        
        internal void add(T item)
        {
            Monitor.Enter(qLock);

            q.Enqueue(item);

            // if the queue count was previously zero, we were
            // waiting, so signal.
            if (q.Count <= 1)
            {
                Monitor.Pulse(qLock);
            }

            Monitor.Exit(qLock);
        }

        internal void close()
        {
            Monitor.Enter(qLock);

            finished = true;
            Monitor.Pulse(qLock);

            Monitor.Exit(qLock);
        }

        internal int Count
        {
            get
            {
                int rv;

                Monitor.Enter(qLock);
                rv = q.Count;
                Monitor.Exit(qLock);

                return rv;
            }
        }

    } // class Channel

}

