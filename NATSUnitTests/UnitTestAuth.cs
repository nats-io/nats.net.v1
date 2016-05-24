// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NATS.Client;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    [TestClass]
    public class TestAuthorization
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

        [TestMethod]
        public void TestAuthSuccess()
        {
            using (NATSServer s = util.CreateServerWithConfig(TestContext, "auth_1222.conf"))
            {
                IConnection c = new ConnectionFactory().CreateConnection("nats://username:password@localhost:1222");
                c.Close();
            }
        }

        [TestMethod]
        public void TestAuthFailure()
        {
            try
            {
                using (NATSServer s = util.CreateServerWithConfig(TestContext, "auth_1222.conf"))
                {
                    connectAndFail("nats://username@localhost:1222");
                    connectAndFail("nats://username:badpass@localhost:1222");
                    connectAndFail("nats://localhost:1222");
                    connectAndFail("nats://badname:password@localhost:1222");
                }
            }
            catch (Exception e)
            {
                System.Console.WriteLine(e);
                throw e;
            }
        }

        [TestMethod]
        public void TestAuthToken()
        {
            try
            {
                using (NATSServer s = util.CreateServerWithArgs(TestContext, "-auth S3Cr3T0k3n!"))
                {
                    connectAndFail("nats://localhost:4222");
                    connectAndFail("nats://invalid_token@localhost:4222");

                    new ConnectionFactory().CreateConnection("nats://S3Cr3T0k3n!@localhost:4222").Close();
                }
            }
            catch (Exception e)
            {
                System.Console.WriteLine(e);
                throw e;
            }
        }


        [TestMethod]
        public void TestReconnectAuthTimeout()
        {
            ConditionalObj obj = new ConditionalObj();

            using (NATSServer s1 = util.CreateServerWithConfig(TestContext, "auth_1222.conf"),
                              s2 = util.CreateServerWithConfig(TestContext, "auth_1223_timeout.conf"),
                              s3 = util.CreateServerWithConfig(TestContext, "auth_1224.conf"))
            {

                Options opts = ConnectionFactory.GetDefaultOptions();

                opts.Servers = new string[]{
                    "nats://username:password@localhost:1222",
                    "nats://username:password@localhost:1223", 
                    "nats://username:password@localhost:1224" };
                opts.NoRandomize = true;

                opts.ReconnectedEventHandler += (sender, args) =>
                {
                    System.Console.WriteLine("Reconnected.");
                    obj.notify();
                };

                opts.DisconnectedEventHandler += (sender, args) =>
                {
                    System.Console.WriteLine("Disconnected.");
                };

                IConnection c = new ConnectionFactory().CreateConnection(opts);

                s1.Shutdown();

                // This should fail over to S2 where an authorization timeout occurs
                // then successfully reconnect to S3.

                obj.wait(20000);
            }
        }
    }
}
