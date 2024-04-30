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
using System.Linq;
using System.Net.Security;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using NATS.Client;
using UnitTests;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests
{
    public class TestTlsProxy : TestSuite<AutoServerSuiteContext>
    {
        public TestTlsProxy(AutoServerSuiteContext context) : base(context)
        {
        }

        // A hack to avoid issues with our test self signed cert.
        // We don't want to require the runner of the test to install the 
        // self signed CA, so we just manually compare the server cert
        // with the what the gnatsd server should return to the client
        // in our test.
        //
        // Getting here means SSL is working in the client.
        //
        private bool verifyServerCert(object sender,
            X509Certificate certificate, X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            string path = UnitTestUtilities.GetFullCertificatePath("server-cert.pem");
            X509Certificate serverCert = new X509Certificate(path);

            // UNSAFE hack for testing purposes.
#if NET46
            return serverCert.GetRawCertDataString().Equals(certificate.GetRawCertDataString());
#else
            return serverCert.Issuer.Equals(certificate.Issuer);
#endif
        }

        /*
            1. client tls first      | secure proxy | server insecure      -> connects
            2. client tls first      | secure proxy | server tls required  -> connects
            3. client tls first      | secure proxy | server tls available -> connects
            4. client regular secure | secure proxy | server insecure      -> mismatch exception
            5. client regular secure | secure proxy | server tls required  -> connects
            6. client regular secure | secure proxy | server tls available -> connects
        */

        const int ServerInsecure = 1;
        const int ServerTlsAvailable = 2;
        const int ServerTlsRequired = 3;

        class ProxyConnection : Connection
        {
            private int ServerType;

            public ProxyConnection(int serverType, Options opts) : base(opts)
            {
                ServerType = serverType;
            }

            internal override void processInfo(string infoJson, bool notify)
            {
                switch (ServerType)
                {
                    case ServerInsecure:
                        base.processInfo(infoJson.Replace(",\"tls_required\":true", ""), notify);
                        return;
                    case ServerTlsRequired:
                        base.processInfo(infoJson.Replace("\"tls_required\":true", "\"tls_available\":true"), notify);
                        return;
                }

                base.processInfo(infoJson, notify);
            }
        }

        private Options MakeMiddleman(TestServerInfo tsi, bool tlsFirst)
        {
            Options opts = Context.GetTestOptions(tsi.Port);
            opts.Secure = true;
            opts.TLSRemoteCertificationValidationCallback = verifyServerCert;
            opts.TlsFirst = tlsFirst;
            opts.MaxReconnect = 0;
            return opts;
        }

        private Connection CreateConnection(int serverType, Options opts)
        {
            Connection nc = new ProxyConnection(serverType, opts);
            try
            {
                nc.connect(false);
            }
            catch (Exception)
            {
                nc.Dispose();
                throw;
            }

            return nc;
        }

        private void CheckConnection(Connection c)
        {
            using (ISyncSubscription s = c.SubscribeSync("foo"))
            {
                c.Publish("foo", null);
                c.Flush();
                Assert.NotNull(s.NextMessage(1000));
            }
        }
        
        [Fact]
        public void TestProxyTlsFirst()
        {
            TestServerInfo tsi = Context.AutoServer();
            using (NATSServer srv = NATSServer.CreateWithConfig(tsi.Port, "tls_first.conf"))
            {
                Options opts = MakeMiddleman(tsi, true);
                using (Connection nc = CreateConnection(ServerTlsRequired, opts))
                {
                    CheckConnection(nc);
                }
                using (Connection nc = CreateConnection(ServerInsecure, opts))
                {
                    CheckConnection(nc);
                }
                using (Connection nc = CreateConnection(ServerTlsAvailable, opts))
                {
                    CheckConnection(nc);
                }
            }
        }
        
        [Fact]
        public void TestProxyNotTlsFirst()
        {
            TestServerInfo tsi = Context.AutoServer();
            using (NATSServer srv = NATSServer.CreateWithConfig(tsi.Port, "tls.conf"))
            {
                Options opts = MakeMiddleman(tsi, false);
                using (Connection nc = CreateConnection(ServerTlsRequired, opts))
                {
                    CheckConnection(nc);
                }
                using (Connection nc = CreateConnection(ServerTlsAvailable, opts))
                {
                    CheckConnection(nc);
                }
                Assert.Throws<NATSSecureConnWantedException>(() => CreateConnection(ServerInsecure, opts));
            }
        }
    }
}
