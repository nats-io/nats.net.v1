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

namespace NATS.Client
{
    // Tracks individual backend servers.
    internal class Srv
    {
        internal Uri url = null;
        internal bool didConnect = false;
        internal int reconnects = 0;
        internal DateTime lastAttempt = DateTime.Now;
        internal bool isImplicit = false;

        // never create a srv object without a url.
        private Srv() { }

        internal Srv(string urlString)
        {
            // allow for host:port, without the prefix.
            if (urlString.ToLower().StartsWith("nats://") == false)
                urlString = "nats://" + urlString;

            url = new Uri(urlString);
        }

        internal Srv(string urlString, bool isUrlImplicit) : this(urlString)
        {
            isImplicit = isUrlImplicit;
        }

        internal void updateLastAttempt()
        {
            lastAttempt = DateTime.Now;
        }

        internal TimeSpan TimeSinceLastAttempt
        {
            get
            {
                return (DateTime.Now - lastAttempt);
            }
        }
    }
}

