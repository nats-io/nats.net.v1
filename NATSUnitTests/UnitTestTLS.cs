// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NATS.Client;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    [TestClass]
    public class TestTLS
    {

        private TestContext testContextInstance;
        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        int hitDisconnect;

        UnitTestUtilities util = new UnitTestUtilities();

        private void connectAndFail(String url)
        {
            try
            {
                System.Console.WriteLine("Trying: " + url);

                hitDisconnect = 0;
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Url = url;
                opts.DisconnectedEventHandler += handleDisconnect;
                IConnection c = new ConnectionFactory().CreateConnection(url);

                Assert.Fail("Expected a failure; did not receive one");
                
                c.Close();
            }
            catch (Exception e)
            {
                if (e.Message.Contains("Authorization"))
                {
                    System.Console.WriteLine("Success with expected failure: " + e.Message);
                }
                else
                {
                    Assert.Fail("Unexpected exception thrown: " + e);
                }
            }
            finally
            {
                if (hitDisconnect > 0)
                    Assert.Fail("The disconnect event handler was incorrectly invoked.");
            }
        }

        private void handleDisconnect(object sender, ConnEventArgs e)
        {
            hitDisconnect++;
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
                    UnitTestUtilities.GetFullCertificatePath(
                    TestContext, "server-cert.pem"));

            // UNSAFE hack for testing purposes.
            if (serverCert.GetRawCertDataString().Equals(certificate.GetRawCertDataString()))
                return true;

            return false;
        }

        [TestMethod]
        public void TestTlsSuccessWithCert()
        {
            using (NATSServer srv = util.CreateServerWithConfig(TestContext, "tls_1222_verify.conf"))
            {
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Secure = true;
                opts.Url = "nats://localhost:1222";
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // .NET requires the private key and cert in the 
                //  same file. 'client.pfx' is generated from:
                //
                // openssl pkcs12 -export -out client.pfx 
                //    -inkey client-key.pem -in client-cert.pem
                X509Certificate2 cert = new X509Certificate2(
                    UnitTestUtilities.GetFullCertificatePath(
                        TestContext, "client.pfx"), "password");

                opts.AddCertificate(cert);

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", null);
                        c.Flush();
                        Msg m = s.NextMessage();
                        System.Console.WriteLine("Received msg over TLS conn.");
                    }
                }
            }
        }

        [TestMethod]
        public void TestTlsFailWithCert()
        {
            using (NATSServer srv = util.CreateServerWithConfig(TestContext, "tls_1222_verify.conf"))
            {
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Secure = true;
                opts.Url = "nats://localhost:1222";
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // this will fail, because it's not complete - missing the private
                // key.
                opts.AddCertificate(UnitTestUtilities.GetFullCertificatePath(
                        TestContext, "client-cert.pem"));

                try
                {
                    new ConnectionFactory().CreateConnection(opts);
                }
                catch (NATSException nae)     
                {
                    System.Console.WriteLine("Caught expected exception: " + nae.Message);
                    return;
                }

                Assert.Fail("Did not receive exception.");
            }
        }

        [TestMethod]
        public void TestTlsFailWithBadAuth()
        {
            using (NATSServer srv = util.CreateServerWithConfig(TestContext, "tls_1222_user.conf"))
            {
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Secure = true;
                opts.Url = "nats://username:BADDPASSOWRD@localhost:1222";
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // this will fail, because it's not complete - missing the private
                // key.
                opts.AddCertificate(UnitTestUtilities.GetFullCertificatePath(
                        TestContext, "client-cert.pem"));

                try
                {
                    new ConnectionFactory().CreateConnection(opts);
                }
                catch (NATSException nae)
                {
                    System.Console.WriteLine("Caught expected exception: " + nae.Message);
                    System.Console.WriteLine("Exception output:" + nae);
                    return;
                }

                Assert.Fail("Did not receive exception.");
            }
        }

        [TestMethod]
        public void TestTlsSuccessSecureConnect()
        {
            using (NATSServer srv = util.CreateServerWithConfig(TestContext, "tls_1222.conf"))
            {
                // we can't call create secure connection w/ the certs setup as they are
                // so we'll override the 
                Options opts = ConnectionFactory.GetDefaultOptions();
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
                        System.Console.WriteLine("Received msg over TLS conn.");
                    }
                }
            }
        }

        [TestMethod]
        public void TestTlsReconnect()
        {
            ConditionalObj reconnectedObj = new ConditionalObj();

            using (NATSServer srv = util.CreateServerWithConfig(TestContext, "tls_1222.conf"),
                   srv2 = util.CreateServerWithConfig(TestContext, "tls_1224.conf"))
            {
                System.Threading.Thread.Sleep(1000);
                // we can't call create secure connection w/ the certs setup as they are
                // so we'll override the 
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Secure = true;
                opts.NoRandomize = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;
                opts.Servers = new string[]{ "nats://localhost:1222" , "nats://localhost:1224" };
                opts.ReconnectedEventHandler += (sender, obj) =>
                {
                    Console.WriteLine("Reconnected.");
                    reconnectedObj.notify();
                };

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    // send a message to be sure.
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", null);
                        c.Flush();
                        s.NextMessage();
                        System.Console.WriteLine("Received msg over TLS conn.");

                        // shutdown the server
                        srv.Shutdown();

                        // wait for reconnect
                        reconnectedObj.wait(30000);

                        c.Publish("foo", null);
                        c.Flush();
                        s.NextMessage();
                        System.Console.WriteLine("Received msg over TLS conn.");
                    }
                }
            }
        }
    }
}
