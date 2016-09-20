using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace NATS.Client
{
    internal sealed class ServerPool
    {
        private object poolLock = new object();
        private LinkedList<Srv> sList = new LinkedList<Srv>();
        private Srv currentServer = null;

        // Used to find duplicates in the server pool.
        // Loopback is equivalent to localhost, and
        // a URL match is equivalent.
        private class SrvEqualityComparer : IEqualityComparer<Srv>
        {
            private bool isLocal(Uri url)
            {
                if (url.IsLoopback)
                    return true;

                if (url.Host.Equals("localhost"))
                    return true;

                return false;
            }

            public bool Equals(Srv x, Srv y)
            {
                if (x == y)
                    return true;

                if (x == null || y == null)
                    return false;

                if (x.url.Equals(y.url))
                    return true;

                if (isLocal(x.url) && isLocal(y.url) && (y.url.Port == x.url.Port))
                    return true;

                return false;
            }

            public int GetHashCode(Srv obj)
            {
                return obj.url.OriginalString.GetHashCode();
            }
        }

        private SrvEqualityComparer duplicateSrvCheck = new SrvEqualityComparer();

        // Create the server pool using the options given.
        // We will place a Url option first, followed by any
        // Server Options. We will randomize the server pool unlesss
        // the NoRandomize flag is set.
        internal void Setup(Options opts)
        {
            if (opts.Servers != null)
            {
                Add(opts.Servers);

                if (!opts.NoRandomize)
                    Shuffle();
            }

            if (!string.IsNullOrWhiteSpace(opts.Url))
                add(opts.Url);

            // Place default URL if pool is empty.
            if (isEmpty())
                add(Defaults.Url);
        }

        // Used for initially connecting to a server.
        internal void ConnectToAServer(Predicate<Srv> connectToServer)
        {
            Srv s;

            // Access the srvPool via index.  SrvPool can theoretically grow
            // if a connection is made, info processed, then disconnected.
            // The ServerPool index operation is threadsafe to account for this.
            for (int i = 0; (s = this[i]) != null; i++)
            {
                if (connectToServer(s))
                {
                    s.didConnect = true;
                    s.reconnects = 0;
                    CurrentServer = s;
                    break;
                }
            }
        }

        // Created for "threadsafe" access.  It allows the list to grow while
        // being added to.
        private Srv this[int index]
        {
            get
            {
                lock (poolLock)
                {
                    if (index + 1 > sList.Count)
                        return null;

                    return sList.ElementAt(index);
                }
            }
        }

        // Sets the currently selected server
        internal Srv CurrentServer
        {
            set
            {
                lock (poolLock)
                {
                    currentServer = value;

                    // a server was removed in the meantime, add it back.
                    if (sList.Contains(currentServer) == false)
                    {
                        add(currentServer);
                    }
                }
            }
        }

        // Pop the current server and put onto the end of the list. 
        // Select head of list as long as number of reconnect attempts
        // under MaxReconnect.
        internal Srv SelectNextServer(int maxReconnect)
        {
            lock (poolLock)
            {
                Srv s = currentServer;
                if (s == null)
                    return null;

                int num = sList.Count;

                // remove the current server.
                sList.Remove(s);

                if (maxReconnect > 0 && s.reconnects < maxReconnect)
                {
                    // if we haven't surpassed max reconnects, add it
                    // to try again.
                    sList.AddLast(s);
                }

                if (isEmpty())
                {
                    return null;
                }

                currentServer = sList.First();

                return currentServer;
            }
        }

        // returns a copy of the list to ensure threadsafety.
        internal string[] GetServerList()
        {
            List<Srv> list;

            lock (poolLock)
            {
                list = new List<Srv>(sList);
            }

            if (list.Count == 0)
                return null;

            var ary = new string[list.Count];
            for (int i = 0; i < list.Count; i++)
            {
                ary[i] = list[i].url.OriginalString;
            }

            return ary;
        }

        private bool add(string s)
        {
            return add(new Srv(s));
        }

        // returns true if it modified the pool, false if
        // the url already exists.
        private bool add(Srv s)
        {
            lock (poolLock)
            {
                if (sList.Contains(s, duplicateSrvCheck))
                    return false;

                sList.AddLast(s);
                return true;
            }
        }

        internal bool Add(string[] urls)
        {
            if (urls == null)
                return false;

            bool modified = false;

            foreach (string s in urls)
            {
                if (add(s))
                {
                    modified = true;
                }
            }

            return modified;
        }

        internal void Shuffle()
        {
            Random r = new Random();

            lock (poolLock)
            {
                var servers = sList.ToArray();
                int n = servers.Length;
                while (n > 1)
                {
                    n--;
                    int k = r.Next(n + 1);
                    var value = servers[k];
                    servers[k] = servers[n];
                    servers[n] = value;
                }

                sList.Clear();
                foreach (Srv s in servers)
                {
                    sList.AddLast(s);
                }
            }
        }

        private bool isEmpty()
        {
            return sList.Count == 0;
        }

        // It'd be possible to use the sList enumerator here and
        // implement the IEnumerable interface, but keep it simple
        // for thread safety.
        internal Srv First()
        {
            lock (poolLock)
            {
                if (sList.Count == 0)
                    return null;

                return sList.First();
            }
        }
    }

}
