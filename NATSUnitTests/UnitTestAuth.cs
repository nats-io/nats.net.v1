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
using NATS.Client;
using System.Threading;
using System.Reflection;
using System.IO;
using System.Linq;
using Xunit;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestAuthorization
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
                IConnection c = new ConnectionFactory().CreateConnection(url);
                Assert.True(false, "Expected a failure; did not receive one");

                c.Close();
            }
            catch (Exception e)
            {
                Assert.Contains("Authorization", e.Message);
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

        [Fact]
        public void TestAuthSuccess()
        {
            using (NATSServer s = util.CreateServerWithConfig("auth_1222.conf"))
            {
                IConnection c = new ConnectionFactory().CreateConnection("nats://username:password@localhost:1222");
                c.Close();
            }
        }

        [Fact]
        public void TestAuthFailure()
        {
            using (NATSServer s = util.CreateServerWithConfig("auth_1222.conf"))
            {
                connectAndFail("nats://username@localhost:1222");
                connectAndFail("nats://username:badpass@localhost:1222");
                connectAndFail("nats://localhost:1222");
                connectAndFail("nats://badname:password@localhost:1222");
            }
        }

        [Fact]
        public void TestAuthToken()
        {
            using (NATSServer s = util.CreateServerWithArgs("-auth S3Cr3T0k3n!"))
            {
                connectAndFail("nats://localhost:4222");
                connectAndFail("nats://invalid_token@localhost:4222");

                new ConnectionFactory().CreateConnection("nats://S3Cr3T0k3n!@localhost:4222").Close();
            }
        }


        [Fact]
        public void TestReconnectAuthTimeout()
        {
            AutoResetEvent ev  = new AutoResetEvent(false);

            using (NATSServer s1 = util.CreateServerWithConfig("auth_1222.conf"),
                              s2 = util.CreateServerWithConfig("auth_1223_timeout.conf"),
                              s3 = util.CreateServerWithConfig("auth_1224.conf"))
            {

                Options opts = util.DefaultTestOptions;

                opts.Servers = new string[]{
                    "nats://username:password@localhost:1222",
                    "nats://username:password@localhost:1223",
                    "nats://username:password@localhost:1224" };
                opts.NoRandomize = true;

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
        public void TestReconnectAuthTimeoutLateClose()
        {
            AutoResetEvent ev = new AutoResetEvent(false);

            using (NATSServer s1 = util.CreateServerWithConfig("auth_1222.conf"),
                              s2 = util.CreateServerWithConfig("auth_1224.conf"))
            {

                Options opts = util.DefaultTestOptions;

                opts.Servers = new string[]{
                    "nats://username:password@localhost:1222",
                    "nats://username:password@localhost:1224" };
                opts.NoRandomize = true;

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
