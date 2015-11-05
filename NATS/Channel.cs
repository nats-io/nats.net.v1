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
    // Roll our own Channels - Concurrent Bag is more heavyweight
    // than we need.
    internal sealed class Channel<T>
    {
        Queue<T> q;
        Object     qLock = new Object();
        bool       finished = false;
        
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
            lock (qLock)
            {
                if (finished)
                    return default(T);

                if (q.Count > 0)
                {
                    return q.Dequeue();
                }
                else
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

                    // we waited..
                    if (finished)
                        return default(T);

                    return q.Dequeue();
                }
            }

        } // get
        
        internal void add(T item)
        {
            lock (qLock)
            {
                q.Enqueue(item);

                // if the queue count was previously zero, we were
                // waiting, so signal.
                if (q.Count <= 1)
                {
                    Monitor.Pulse(qLock);
                }
            }
        }

        internal void close()
        {
            lock (qLock)
            {
                finished = true;
                Monitor.Pulse(qLock);
            }
        }

        internal int Count
        {
            get
            {
                lock (qLock)
                {
                    return q.Count;
                }
            }
        }

    } // class Channel

}

