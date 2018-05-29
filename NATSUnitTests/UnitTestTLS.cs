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

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestTLS
    {
        int hitDisconnect;

        UnitTestUtilities util = new UnitTestUtilities();

        private void connectAndFail(String url)
        {
            try
            {
                hitDisconnect = 0;
                Options opts = util.DefaultTestOptions;
                opts.Url = url;
                opts.DisconnectedEventHandler += handleDisconnect;
                IConnection c = null;

                Assert.ThrowsAny<Exception>(() => c = new ConnectionFactory().CreateConnection(url));
                
                c.Close();
            }
            catch (Exception e)
            {
                Assert.Contains(e.Message, "Authorization");
            }
            finally
            {
                Assert.False(hitDisconnect > 0, "The disconnect event handler was incorrectly invoked.");
            }
        }

        private void handleDisconnect(object sender, ConnEventArgs e)
        {
            hitDisconnect++;
        }

        private bool compareBytes(byte[] bs1, byte[] bs2)
        {
            if (bs1.Length == bs2.Length)
                return false;

            for (int i = bs1.Length; i < bs1.Length; i++)
            {
                if (bs1[i] != bs2[i])
                    return false;
            }

            return true;
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

            X509Certificate serverCert = new X509Certificate(
                    UnitTestUtilities.GetFullCertificatePath("server-cert.pem"));

            // UNSAFE hack for testing purposes.
#if NET45
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
            using (NATSServer srv = util.CreateServerWithConfig("tls_1222_verify.conf"))
            {
                Options opts = util.DefaultTestOptions;
                opts.Secure = true;
                opts.Url = "nats://localhost:1222";
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // .NET requires the private key and cert in the 
                //  same file. 'client.pfx' is generated from:
                //
                // openssl pkcs12 -export -out client.pfx 
                //    -inkey client-key.pem -in client-cert.pem
                X509Certificate2 cert = new X509Certificate2(
                    UnitTestUtilities.GetFullCertificatePath("client.pfx"), "password");

                opts.AddCertificate(cert);

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
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
            using (NATSServer srv = util.CreateServerWithConfig("tls_1222_verify.conf"))
            {
                Options opts = util.DefaultTestOptions;
                opts.Secure = true;
                opts.Url = "nats://localhost:1222";
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // this will fail, because it's not complete - missing the private
                // key.
                opts.AddCertificate(UnitTestUtilities.GetFullCertificatePath("client-cert.pem"));

                Assert.ThrowsAny<NATSException>(() => new ConnectionFactory().CreateConnection(opts));
            }
        }

        [Fact]
        public void TestTlsFailWithBadAuth()
        {
            using (NATSServer srv = util.CreateServerWithConfig("tls_1222_user.conf"))
            {
                Options opts = util.DefaultTestOptions;
                opts.Secure = true;
                opts.Url = "nats://username:BADDPASSOWRD@localhost:1222";
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // this will fail, because it's not complete - missing the private
                // key.
                opts.AddCertificate(UnitTestUtilities.GetFullCertificatePath("client-cert.pem"));

                Assert.ThrowsAny<NATSException>(() => new ConnectionFactory().CreateConnection(opts));
            }
        }

        [Fact]
        public void TestTlsSuccessSecureConnect()
        {
            using (NATSServer srv = util.CreateServerWithConfig("tls_1222.conf"))
            {
                // we can't call create secure connection w/ the certs setup as they are
                // so we'll override the 
                Options opts = util.DefaultTestOptions;
                opts.Secure = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;
                opts.Url = "nats://localhost:1222";

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
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

            using (NATSServer srv = util.CreateServerWithConfig("tls_1222.conf"),
                   srv2 = util.CreateServerWithConfig("tls_1224.conf"))
            {
                Thread.Sleep(1000);

                Options opts = util.DefaultTestOptions;
                opts.Secure = true;
                opts.NoRandomize = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;
                opts.Servers = new string[]{ "nats://localhost:1222" , "nats://localhost:1224" };
                opts.ReconnectedEventHandler += (sender, obj) =>
                {
                    ev.Set();
                };

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
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

            using (NATSServer s1 = util.CreateServerWithConfig("auth_tls_1222.conf"),
                              s2 = util.CreateServerWithConfig("auth_tls_1223_timeout.conf"),
                              s3 = util.CreateServerWithConfig("auth_tls_1224.conf"))
            {

                Options opts = util.DefaultTestOptions;
                opts.Secure = true;
                opts.NoRandomize = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                opts.Servers = new string[]{
                    "nats://username:password@localhost:1222",
                    "nats://username:password@localhost:1223", 
                    "nats://username:password@localhost:1224" };

                opts.ReconnectedEventHandler += (sender, args) =>
                {
                    ev.Set();
                };

                IConnection c = new ConnectionFactory().CreateConnection(opts);
                s1.Shutdown();

                // This should fail over to S2 where an authorization timeout occurs
                // then successfully reconnect to S3.

                Assert.True(ev.WaitOne(20000));
            }
        }

#if NET45
        [Fact]
        public void TestTlsReconnectAuthTimeoutLateClose()
        {
            AutoResetEvent ev = new AutoResetEvent(false);

            using (NATSServer s1 = util.CreateServerWithConfig("auth_tls_1222.conf"),
                              s2 = util.CreateServerWithConfig("auth_tls_1224.conf"))
            {

                Options opts = util.DefaultTestOptions;
                opts.Secure = true;
                opts.NoRandomize = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                opts.Servers = new string[]{
                    "nats://username:password@localhost:1222",
                    "nats://username:password@localhost:1224" };

                opts.ReconnectedEventHandler += (sender, args) =>
                {
                    ev.Set();
                };

                IConnection c = new ConnectionFactory().CreateConnection(opts);

                // inject an authorization timeout, as if it were processed by an incoming server message.
                // this is done at the parser level so that parsing is also tested,
                // therefore it needs reflection since Parser is an internal type.
                Type parserType = typeof(Connection).Assembly.GetType("NATS.Client.Parser");
                Assert.NotNull(parserType);

                BindingFlags flags = BindingFlags.NonPublic | BindingFlags.Instance;
                object parser = Activator.CreateInstance(parserType, flags, null, new object[] { c }, null);
                Assert.NotNull(parser);

                MethodInfo parseMethod = parserType.GetMethod("parse", flags);
                Assert.NotNull(parseMethod);

                byte[] bytes = "-ERR 'Authorization Timeout'\r\n".ToCharArray().Select(ch => (byte)ch).ToArray();
                parseMethod.Invoke(parser, new object[] { bytes, bytes.Length });

                // sleep to allow the client to process the error, then shutdown the server.
                Thread.Sleep(250);
                s1.Shutdown();

                // Wait for a reconnect.
                Assert.True(ev.WaitOne(20000));
            }
        }
#endif
    }
}
