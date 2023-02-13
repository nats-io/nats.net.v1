// Copyright 2023 The NATS Authors
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
using NATS.Client.Internals;

namespace NATS.Client
{
    internal sealed class NatsServerListProvider : IServerListProvider
    {
        private Options _options;
        private readonly List<NatsUri> _list;

        public NatsServerListProvider()
        {
            _list = new List<NatsUri>();
        }

        public void Initialize(Options opts)
        {
            this._options = opts;
            foreach (NatsUri nuri in opts.ServerUris)
            {
                _list.Add(nuri);
            }
        }

        public bool AcceptDiscoveredServers(string[] discoveredServers)
        {
            bool anyAdded = false;
            if (!_options.IgnoreDiscoveredServers) {
                // TODO PRUNE ???
                foreach (string discovered in discoveredServers) {
                    try
                    {
                        anyAdded |= Add(new NatsUri(discovered));
                    }
                    catch (Exception)
                    {
                        // should never actually happen
                    }
                }
            }
            return anyAdded;
        }

        public IList<NatsUri> GetServersToTry(NatsUri lastConnectedServer)
        {
            if (_list.Count > 1) {
                bool removed = false;
                if (lastConnectedServer != null) {
                    removed = _list.Remove(lastConnectedServer);
                }
                if (!_options.NoRandomize && _list.Count > 1) {
                    MiscUtils.ShuffleInPlace(_list);
                }
                if (removed) {
                    _list.Add(lastConnectedServer);
                }
            }
            return new List<NatsUri>(_list); // return a copy
        }

        private bool Add(NatsUri nuri) {
            // TODO ResolveHostnames System.Net.Dns not available, ask for help
            // if (_options.ResolveHostnames) {
            //     if (nuri.HostIsIpAddress) {
            //         return _add(nuri);
            //     }
            //
            //     bool added = false;
            //     try {
            //         IPAddress[] addresses = System.Net.Dns.GetHostAddresses(nuri.Host);
            //         foreach (IPAddress a in addresses)
            //         {
            //             try {
            //                 added |= _add(nuri.ReHost(a.ToString()));
            //             }
            //             catch (Exception ignore) {
            //                 // should never actually happen
            //             }
            //         }
            //     }
            //     catch (Exception ignore) {
            //         // A user might have supplied a bad host, but the server shouldn't.
            //         // Either way, nothing much to do.
            //     }
            //     return added;
            // }
            return _add(nuri);
        }

        private bool _add(NatsUri nuri) {
            if (_list.Contains(nuri)) {
                return false;
            }
            _list.Add(nuri);
            return true;
        }
    }
}
