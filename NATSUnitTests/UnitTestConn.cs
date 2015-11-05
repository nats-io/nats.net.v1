// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NATS.Client;
using System.Threading;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    [TestClass]
    public class TestConnection
    {

        UnitTestUtilities utils = new UnitTestUtilities();

        [TestInitialize()]
        public void Initialize()
        {
            utils.StartDefaultServer();
        }

        [TestCleanup()]
        public void Cleanup()
        {
            utils.StopDefaultServer();
        }

        [TestMethod]
        public void TestConnectionStatus()
        {
            IConnection c = new ConnectionFactory().CreateConnection();
            Assert.AreEqual(ConnState.CONNECTED, c.State);
            c.Close();
            Assert.AreEqual(ConnState.CLOSED, c.State);
        }

        [TestMethod]
        public void TestCloseHandler()
        {
            bool closed = false;

            Options o = ConnectionFactory.GetDefaultOptions();
            o.ClosedEventHandler += (sender, args) => { closed = true; };
            IConnection c = new ConnectionFactory().CreateConnection(o);
            c.Close();
            Assert.IsTrue(closed);

            // now test using.
            closed = false;
            using (c = new ConnectionFactory().CreateConnection(o)) { };
            Assert.IsTrue(closed);
        }

        [TestMethod]
        public void TestCloseDisconnectedHandler()
        {
            bool disconnected = false;
            Object mu = new Object();

            Options o = ConnectionFactory.GetDefaultOptions();
            o.AllowReconnect = false;
            o.DisconnectedEventHandler += (sender, args) => {
                lock (mu)
                {
                    disconnected = true;
                    Monitor.Pulse(mu);
                }
            };

            IConnection c = new ConnectionFactory().CreateConnection(o);
            lock (mu)
            {
                c.Close();
                Monitor.Wait(mu, 20000);
            }
            Assert.IsTrue(disconnected);

            // now test using.
            disconnected = false;
            lock (mu)
            {
                using (c = new ConnectionFactory().CreateConnection(o)) { };
                Monitor.Wait(mu);
            }
            Assert.IsTrue(disconnected);
        }

        [TestMethod]
        public void TestServerStopDisconnectedHandler()
        {
            bool disconnected = false;
            Object mu = new Object();

            Options o = ConnectionFactory.GetDefaultOptions();
            o.AllowReconnect = false;
            o.DisconnectedEventHandler += (sender, args) =>
            {
                lock (mu)
                {
                    disconnected = true;
                    Monitor.Pulse(mu);
                }
            };

            IConnection c = new ConnectionFactory().CreateConnection(o);
            lock (mu)
            {
                utils.bounceDefaultServer(1000);
                Monitor.Wait(mu);
            }
            c.Close();
            Assert.IsTrue(disconnected);
        }

        [TestMethod]
        public void TestClosedConnections()
        {
            IConnection c = new ConnectionFactory().CreateConnection();
            ISyncSubscription s = c.SubscribeSync("foo");

            c.Close();

            // While we can annotate all the exceptions in the test framework,
            // just do it manually.
            UnitTestUtilities.testExpectedException(
                () => { c.Publish("foo", null); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.Publish(new Msg("foo")); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.SubscribeAsync("foo"); },
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.SubscribeSync("foo"); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.SubscribeAsync("foo", "bar"); },
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.SubscribeSync("foo", "bar"); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.Request("foo", null); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { s.NextMessage(); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { s.NextMessage(100); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { s.Unsubscribe(); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { s.AutoUnsubscribe(1); }, 
                typeof(NATSConnectionClosedException));
        }

        /// NOT IMPLEMENTED:
        /// TestServerSecureConnections
        /// TestErrOnConnectAndDeadlock
        /// TestErrOnMaxPayloadLimit

    } // class

} // namespace
