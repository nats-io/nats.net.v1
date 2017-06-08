// Copyright 2015 Apcera Inc. All rights reserved.

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

