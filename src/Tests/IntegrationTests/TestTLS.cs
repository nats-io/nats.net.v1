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
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using NATS.Client;
using System.Reflection;
using System.IO;
using System.Linq;
using System.Threading;
using Xunit;

namespace IntegrationTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestTls : TestSuite<TlsSuiteContext>
    {
        public TestTls(TlsSuiteContext context) : base(context) { }

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

            X509Certificate serverCert = new X509Certificate(
                    UnitTestUtilities.GetFullCertificatePath("server-cert.pem"));

            // UNSAFE hack for testing purposes.
#if NET46
            var isOK = serverCert.GetRawCertDataString().Equals(certificate.GetRawCertDataString());
#else
            var isOK = serverCert.Issuer.Equals(certificate.Issuer);
#endif
            if (isOK)
                return true;

            return false;
        }

        [Fact]
        public void TestTlsSuccessWithCert()
        {
            using (NATSServer srv = NATSServer.CreateWithConfig(Context.Server1.Port, "tls_verify.conf"))
            {
                Options opts = Context.GetTestOptions(Context.Server1.Port);
                opts.Secure = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // .NET requires the private key and cert in the 
                //  same file. 'client.pfx' is generated from:
                //
                // openssl pkcs12 -export -out client.pfx 
                //    -inkey client-key.pem -in client-cert.pem
                X509Certificate2 cert = new X509Certificate2(
                    UnitTestUtilities.GetFullCertificatePath("client.pfx"), "password");

                opts.AddCertificate(cert);

                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", null);
                        c.Flush();
                        Msg m = s.NextMessage();
                    }
                }
            }
        }

        [Fact]
        public void TestTlsFailWithCert()
        {
            using (NATSServer srv = NATSServer.CreateWithConfig(Context.Server1.Port, "tls_verify.conf"))
            {
                Options opts = Context.GetTestOptions(Context.Server1.Port);
                opts.Secure = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // this will fail, because it's not complete - missing the private
                // key.
                opts.AddCertificate(UnitTestUtilities.GetFullCertificatePath("client-cert.pem"));

                Assert.ThrowsAny<NATSException>(() => Context.ConnectionFactory.CreateConnection(opts));
            }
        }

        // Test verfier to fail on the server cert.
        //
        private bool verifyCertAlwaysFail(object sender,
            X509Certificate certificate, X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            return false;
        }

        [Fact]
        public void TestTlsFailWithInvalidServerCert()
        {
            using (NATSServer srv = NATSServer.CreateWithConfig(Context.Server1.Port, "tls_verify.conf"))
            {
                Options opts = Context.GetTestOptions(Context.Server1.Port);
                opts.Secure = true;
                opts.TLSRemoteCertificationValidationCallback = verifyCertAlwaysFail;

                // this will fail, because it's not complete - missing the private
                // key.
                opts.AddCertificate(UnitTestUtilities.GetFullCertificatePath("client-cert.pem"));

                Assert.ThrowsAny<NATSException>(() => Context.ConnectionFactory.CreateConnection(opts));
            }
        }

        [Fact]
        public void TestTlsFailWithBadAuth()
        {
            using (NATSServer srv = NATSServer.CreateWithConfig(Context.Server1.Port, "tls_user.conf"))
            {
                Options opts = Context.GetTestOptions(Context.Server1.Port);
                opts.Secure = true;
                opts.Url = $"nats://username:BADDPASSOWRD@localhost:{Context.Server1.Port}";
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // this will fail, because it's not complete - missing the private
                // key.
                opts.AddCertificate(UnitTestUtilities.GetFullCertificatePath("client-cert.pem"));

                Assert.ThrowsAny<NATSException>(() => Context.ConnectionFactory.CreateConnection(opts));
            }
        }

        private void TestTLSSecureConnect(bool setSecure)
        {
            using (NATSServer srv = NATSServer.CreateWithConfig(Context.Server1.Port, "tls.conf"))
            {
                // we can't call create secure connection w/ the certs setup as they are
                // so we'll override the validation callback
                Options opts = Context.GetTestOptions(Context.Server1.Port);
                opts.Secure = setSecure;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", null);
                        c.Flush();
                        Msg m = s.NextMessage();
                    }
                }
            }
        }

        [Fact]
        public void TestTlsSuccessSecureConnect()
        {
            TestTLSSecureConnect(true);
        }

        [Fact]
        public void TestTlsSuccessSecureConnectFromServerInfo()
        {
            TestTLSSecureConnect(false);
        }

        [Fact]
        public void TestTlsScheme()
        {
            using (NATSServer srv = NATSServer.CreateWithConfig(Context.Server1.Port, "tls.conf"))
            {
                // we can't call create secure connection w/ the certs setup as they are
                // so we'll override the validation callback
                Options opts = Context.GetTestOptions(Context.Server1.Port);
                opts.Url = $"tls://127.0.0.1:{Context.Server1.Port}";
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", null);
                        c.Flush();
                        Msg m = s.NextMessage();
                    }
                }
            }
        }

        [Fact]
        public void TestTlsReconnect()
        {
            AutoResetEvent ev = new AutoResetEvent(false);

            using (NATSServer
                srv = NATSServer.CreateWithConfig(Context.Server1.Port, "tls.conf"),
                srv2 = NATSServer.CreateWithConfig(Context.Server2.Port, "tls.conf"))
            {
                Thread.Sleep(1000);

                Options opts = Context.GetTestOptions();
                opts.Secure = true;
                opts.NoRandomize = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;
                opts.Servers = new[]{ Context.Server1.Url, Context.Server2.Url };
                opts.ReconnectedEventHandler += (sender, obj) =>
                {
                    ev.Set();
                };

                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    // send a message to be sure.
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", null);
                        c.Flush();
                        s.NextMessage();

                        // shutdown the server
                        srv.Shutdown();

                        // wait for reconnect
                        Assert.True(ev.WaitOne(30000));

                        c.Publish("foo", null);
                        c.Flush();
                        s.NextMessage();
                    }
                }
            }
        }

        // Tests if reconnection can still occur after a user authorization failure
        // under TLS.
        [Fact]
        public void TestTlsReconnectAuthTimeout()
        {
            AutoResetEvent ev = new AutoResetEvent(false);

            using (NATSServer s1 = NATSServer.CreateWithConfig(Context.Server1.Port, "auth_tls.conf"),
                              s2 = NATSServer.CreateWithConfig(Context.Server2.Port, "auth_tls_timeout.conf"),
                              s3 = NATSServer.CreateWithConfig(Context.Server3.Port, "auth_tls.conf"))
            {

                Options opts = Context.GetTestOptions();
                opts.Secure = true;
                opts.NoRandomize = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                opts.Servers = new[]{
                    $"nats://username:password@localhost:{Context.Server1.Port}",
                    $"nats://username:password@localhost:{Context.Server2.Port}", 
                    $"nats://username:password@localhost:{Context.Server3.Port}" };

                opts.ReconnectedEventHandler += (sender, args) =>
                {
                    ev.Set();
                };

                using (Context.ConnectionFactory.CreateConnection(opts))
                {
                    s1.Shutdown();

                    // This should fail over to S2 where an authorization timeout occurs
                    // then successfully reconnect to S3.

                    Assert.True(ev.WaitOne(20000));
                }
            }
        }

#if NET46
        [Fact]
        public void TestTlsReconnectAuthTimeoutLateClose()
        {
            AutoResetEvent ev = new AutoResetEvent(false);

            using (NATSServer s1 = NATSServer.CreateWithConfig(Context.Server1.Port, "auth_tls.conf"),
                              s2 = NATSServer.CreateWithConfig(Context.Server2.Port, "auth_tls.conf"))
            {

                Options opts = Context.GetTestOptions();
                opts.Secure = true;
                opts.NoRandomize = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                opts.Servers = new string[]{
                    $"nats://username:password@localhost:{Context.Server1.Port}",
                    $"nats://username:password@localhost:{Context.Server2.Port}" };

                opts.ReconnectedEventHandler += (sender, args) =>
                {
                    ev.Set();
                };

                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    // inject an authorization timeout, as if it were processed by an incoming server message.
                    // this is done at the parser level so that parsing is also tested,
                    // therefore it needs reflection since Parser is an internal type.
                    Type parserType = typeof(Connection).Assembly.GetType("NATS.Client.Parser");
                    Assert.NotNull(parserType);

                    BindingFlags flags = BindingFlags.NonPublic | BindingFlags.Instance;
                    object parser = Activator.CreateInstance(parserType, flags, null, new object[] {c}, null);
                    Assert.NotNull(parser);

                    MethodInfo parseMethod = parserType.GetMethod("parse", flags);
                    Assert.NotNull(parseMethod);

                    byte[] bytes = "-ERR 'Authorization Timeout'\r\n".ToCharArray().Select(ch => (byte) ch).ToArray();
                    parseMethod.Invoke(parser, new object[] {bytes, bytes.Length});

                    // sleep to allow the client to process the error, then shutdown the server.
                    Thread.Sleep(250);
                    s1.Shutdown();

                    // Wait for a reconnect.
                    Assert.True(ev.WaitOne(20000));
                }
            }
        }
#endif
    }
}
