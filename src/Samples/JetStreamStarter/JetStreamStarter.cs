// Copyright 2022 The NATS Authors
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
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using NATS.Client;
using NATS.Client.Internals;

namespace NATSExamples
{
    internal static class JetStreamStarter
    {
        static void Main(string[] args)
        {
            Options opts = Dbg.GetDefaultOptions();
            // opts.Timeout = 5000;
            opts.Servers = new [] {"tls://localhost:9222", "tls://localhost:4222"};
            // opts.Servers = new [] {"tls://localhost:4222", "tls://localhost:9222"};
            opts.NoRandomize = true;
            opts.TlsFirst = true;
            opts.Secure = true;
            opts.TLSRemoteCertificationValidationCallback = (sender, certificate, chain, errors) =>
            {
                Dbg.dbg("TLSRemoteCertificationValidationCallback");
                return true;
            };

            try
            {
                Dbg.dbg("B4");
                using (IConnection c = new ConnectionFactory().CreateConnection(opts, true))
                {
                    Dbg.dbg("ServerInfo", c.ServerInfo);
                }
            }
            catch (Exception e)
            {
                Dbg.dbg("EX", e);
            }
        }
    }
}