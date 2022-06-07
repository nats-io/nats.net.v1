// Copyright 2015-2022 The NATS Authors
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
    // TODO After connect adr is complete
    internal interface IServerProvider
    {
        /// <summary>
        /// Setup your provider 
        /// </summary>
        /// <param name="opts"></param>
        void Setup(Options opts);
        Srv First();
        Srv SelectNextServer(int maxReconnect);
        string[] GetServerList(bool implicitOnly);
        void ConnectToAServer(Predicate<Srv> connectToServer);
        bool HasSecureServer();
        void SetCurrentServer(Srv value);
        bool AcceptDiscoveredServers(string[] discoveredUrls);
    }

    // Tracks individual backend servers.
    public class Srv
    {
        public const string DefaultScheme = "nats://";
        public const int DefaultPort = 4222;
        public const int NoPortSpecified = -1;
        
        public Uri Url { get; }
        public bool IsImplicit { get; }
        public bool Secure { get; }
        public DateTime LastAttempt { get; private set; }
        public bool DidConnect { get; set; }
        public int Reconnects { get; set; }

        // never create a srv object without a url.
        private Srv() { }

        public Srv(string urlString)
        {
            if (!urlString.Contains("://"))
            {
                urlString = DefaultScheme + urlString;
            }
            else
            {
                Secure = urlString.Contains("tls://");
            }

            var uri = new Uri(urlString);

            Url = uri.Port == NoPortSpecified ? new UriBuilder(uri) {Port = DefaultPort}.Uri : uri;
            
            LastAttempt = DateTime.Now;
        }

        public Srv(string urlString, bool isUrlImplicit) : this(urlString)
        {
            IsImplicit = isUrlImplicit;
        }

        public void UpdateLastAttempt() => LastAttempt = DateTime.Now;

        public TimeSpan TimeSinceLastAttempt => (DateTime.Now - LastAttempt);
    }
}

