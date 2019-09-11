// Copyright 2016-2018 The NATS Authors
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
using System.Linq;

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
                Add(opts.Servers, false);

                if (!opts.NoRandomize)
                    shuffle();
            }

            if (!string.IsNullOrWhiteSpace(opts.Url))
                add(opts.Url, false);

            // Place default URL if pool is empty.
            if (isEmpty())
                add(Defaults.Url, false);
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

                if (maxReconnect == Options.ReconnectForever || 
                   (maxReconnect > 0 && s.reconnects < maxReconnect))
                {
                    // if we haven't surpassed max reconnects, add it
                    // to try again.
                    sList.AddLast(s);
                }

                currentServer = isEmpty() ? null : sList.First();

                return currentServer;
            }
        }

        // returns a copy of the list to ensure threadsafety.
        internal string[] GetServerList(bool implicitOnly)
        {
            List<Srv> list;

            lock (poolLock)
            {
                if (sList.Count == 0)
                    return null;

                list = new List<Srv>(sList);
            }

            if (list.Count == 0)
                return null;

            var rv = new List<string>();
            foreach (Srv s in list)
            {
                if (implicitOnly && !s.isImplicit)
                    continue;

                rv.Add(string.Format("{0}://{1}:{2}", s.url.Scheme, s.url.Host, s.url.Port));
            }

            return rv.ToArray();
        }

        // returns true if it modified the pool, false if
        // the url already exists.
        private bool add(string s, bool isImplicit)
        {
            return add(new Srv(s, isImplicit));
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

        // removes implict servers NOT found in the provided list. 
        internal void PruneOutdatedServers(string[] newUrls)
        {
            LinkedList<string> ulist = new LinkedList<string>(newUrls);

            lock (poolLock)
            {
                var tmp = new Srv[sList.Count];
                sList.CopyTo(tmp, 0);

                // if a server is implicit and cannot be found in the url
                // list the remove it unless we are connected to it.
                foreach (Srv s in tmp)
                {
                    // The server returns "<host>:<port>".  We can't compare
                    // againts Uri.Authority becase that API may strip out 
                    // ports.
                    string hp = string.Format("{0}:{1}", s.url.Host, s.url.Port);
                    if (s.isImplicit && !ulist.Contains(hp) &&
                        s != currentServer)
                    {
                        sList.Remove(s);
                    }
                }
            }
        }

        // returns true if any of the urls were added to the pool,
        // false if they all already existed
        internal bool Add(string[] urls, bool isImplicit)
        {
            if (urls == null)
                return false;

            bool didAdd = false;
            foreach (string s in urls)
            {
                didAdd |= add(s, isImplicit);
            }

            return didAdd;
        }

        // Convenience method to shuffle a list.  The list passed
        // is modified.
        internal static void shuffle<T>(IList<T> list)
        {
            if (list == null)
                return;

            int n = list.Count;
            if (n == 1)
                return;

            Random r = new Random();
            while (n > 1)
            {
                n--;
                int k = r.Next(n + 1);
                var value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }

        private void shuffle()
        {
            lock (poolLock)
            {
                var servers = sList.ToArray();
                shuffle(servers);

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
