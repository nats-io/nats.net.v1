// Copyright 2016-2020 The NATS Authors
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
    internal sealed class ServerPool : IServerProvider
    {
        private readonly object poolLock = new object();
        private readonly LinkedList<Srv> sList = new LinkedList<Srv>();
        private Srv currentServer;
        private readonly Random rand = new Random(DateTime.Now.Millisecond);
        private bool randomize = true;
        private bool ignoreDiscoveredServers = true;

        // Used to find duplicates in the server pool.
        // Loopback is equivalent to localhost, and
        // a URL match is equivalent.
        private class SrvEqualityComparer : IEqualityComparer<Srv>
        {
            private bool IsLocal(Uri url)
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

                if (x.Url.Equals(y.Url))
                    return true;

                if (IsLocal(x.Url) && IsLocal(y.Url) && (y.Url.Port == x.Url.Port))
                    return true;

                return false;
            }

            public int GetHashCode(Srv obj)
            {
                return obj.Url.OriginalString.GetHashCode();
            }
        }

        private SrvEqualityComparer duplicateSrvCheck = new SrvEqualityComparer();

        // Create the server pool using the options given.
        // We will place a Url option first, followed by any
        // Server Options. We will randomize the server pool unless
        // the NoRandomize flag is set.
        public void Setup(Options opts)
        {
            randomize = !opts.NoRandomize;
            ignoreDiscoveredServers = opts.IgnoreDiscoveredServers;

            if (opts.Servers != null)
            {
                Add(opts.Servers, false);
                if (randomize)
                {
                    Shuffle();
                }
            }

            // Place default URL if pool is empty.
            if (IsEmpty())
            {
                Add(Defaults.Url, false);
            }
        }

        // Used for initially connecting to a server.
        public void ConnectToAServer(Predicate<Srv> connectToServer)
        {
            Srv s;

            // Access the srvPool via index.  SrvPool can theoretically grow
            // if a connection is made, info processed, then disconnected.
            // The ServerPool index operation is threadsafe to account for this.
            for (int i = 0; (s = this[i]) != null; i++)
            {
                if (connectToServer(s))
                {
                    s.DidConnect = true;
                    s.Reconnects = 0;
                    SetCurrentServer(s);
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
        public void SetCurrentServer(Srv value)
        {
            lock (poolLock)
            {
                currentServer = value;

                // make sure server is in list, it might have been removed.
                Add(currentServer);
            }
        }

        // Pop the current server and put onto the end of the list. 
        // Select head of list as long as number of reconnect attempts
        // under MaxReconnect.
        public Srv SelectNextServer(int maxReconnect)
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
                   (maxReconnect > 0 && s.Reconnects < maxReconnect))
                {
                    // if we haven't surpassed max reconnects, add it
                    // to try again.
                    sList.AddLast(s);
                }

                currentServer = IsEmpty() ? null : sList.First();

                return currentServer;
            }
        }

        // returns a copy of the list to ensure threadsafety.
        public string[] GetServerList(bool implicitOnly)
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
                if (implicitOnly && !s.IsImplicit)
                    continue;

                rv.Add(string.Format("{0}://{1}:{2}", s.Url.Scheme, s.Url.Host, s.Url.Port));
            }

            return rv.ToArray();
        }

        // returns true if it modified the pool, false if
        // the url already exists.
        private bool Add(string s, bool isImplicit)
        {
            return Add(new Srv(s, isImplicit));
        }

        // returns true if it modified the pool, false if
        // the url already exists.
        private bool Add(Srv s)
        {
            lock (poolLock)
            {
                if (sList.Contains(s, duplicateSrvCheck))
                    return false;

                if (s.IsImplicit && randomize)
                {
                    // pick a random spot to add the server.
                    var randElem = sList.ElementAt(rand.Next(sList.Count));
                    sList.AddAfter(sList.Find(randElem), s);
                }
                else
                {
                    sList.AddLast(s);
                }

                return true;
            }
        }

        // The discoveredUrls array could be empty/not present on initial
        // connect if advertise is disabled on that server, or servers that
        // did not include themselves in the async INFO protocol.
        // If empty, do not remove the implicit servers from the pool.  
        //
        // Note about pool randomization: when the pool was first created,
        // it was randomized (if allowed). We keep the order the same (removing
        // implicit servers that are no longer sent to us). New URLs are sent
        // to us in no specific order so don't need extra randomization.
        //
        // Prune out implicit servers no longer needed.  
        // The Add is idempotent, so just add the entire list.
        public bool AcceptDiscoveredServers(string[] discoveredUrls)
        {
            if (ignoreDiscoveredServers || discoveredUrls == null
                                        || discoveredUrls.Length == 0)
            {
                return false;
            }

            PruneOutdatedServers(discoveredUrls);
            return Add(discoveredUrls, true);
        } 
        
        // removes implicit servers NOT found in the provided list. 
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
                    // against Uri.Authority because that API may strip out 
                    // ports.
                    string hp = string.Format("{0}:{1}", s.Url.Host, s.Url.Port);
                    if (s.IsImplicit && !ulist.Contains(hp) &&
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
                didAdd |= Add(s, isImplicit);
            }

            return didAdd;
        }

        // Convenience method to shuffle a list.  The list passed
        // is modified.
        internal static void Shuffle<T>(IList<T> list)
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

        private void Shuffle()
        {
            lock (poolLock)
            {
                var servers = sList.ToArray();
                Shuffle(servers);

                sList.Clear();
                foreach (Srv s in servers)
                {
                    sList.AddLast(s);
                }
            }
        }

        private bool IsEmpty()
        {
            return sList.Count == 0;
        }

        // It'd be possible to use the sList enumerator here and
        // implement the IEnumerable interface, but keep it simple
        // for thread safety.
        public Srv First()
        {
            lock (poolLock)
            {
                if (sList.Count == 0)
                    return null;

                return sList.First();
            }
        }

        public bool HasSecureServer()
        {
            lock (poolLock)
            {
                foreach (Srv s in sList)
                {
                    if (s.Secure)
                        return true;
                }
            }
            return false;
        }
    }

}
