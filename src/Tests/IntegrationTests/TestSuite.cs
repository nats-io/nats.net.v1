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
using System.Collections.Generic;
using System.Linq;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;

namespace IntegrationTests
{
    public abstract class TestSuite<TSuiteContext> : IClassFixture<TSuiteContext> where TSuiteContext : class
    {
        protected TSuiteContext Context { get; }

        protected TestSuite(TSuiteContext context)
        {
            Context = context;
        }
    }

    /// <summary>
    /// IANA unassigned port range 11490-11599 has been selected within the user-ports (1024-49151).
    /// </summary>
    public static class TestSeedPorts
    {
        public const int DefaultSuiteNormalServers = 4221; //7pc
        public const int DefaultSuiteNormalClusterServers = 4551; //7pc

        public const int AuthorizationSuite = 11490; //3pc
        public const int ReconnectSuite = 11493; //1pc
        public const int PublishErrorsDuringReconnectSuite = 11494; //1pc
        public const int ClusterSuite = 11495; //10pc
        public const int ConnectionSuite = 11505;//2pc
        public const int ConnectionSecuritySuite = 11507; //1pc
        public const int ConnectionDrainSuite = 11508; //1pc
        public const int ConnectionMemoryLeaksSuite = 11509; //3pc
        public const int EncodingSuite = 11512; //1pc
        public const int SubscriptionsSuite = 11513; //1pc
        public const int TlsSuite = 11514; //3pc
        public const int RxSuite = 11517; //1pc
        public const int AsyncAwaitDeadlocksSuite = 11518; //1pc
        public const int ConnectionIpV6Suite = 11519; //1pc

        public static InterlockedInt AutoPort = new InterlockedInt(11520);
    }

    public abstract class SuiteContext
    {
        public ConnectionFactory ConnectionFactory { get; } = new ConnectionFactory();

        public Options GetTestOptionsWithDefaultTimeout(int? port = null)
        {
            var opts = ConnectionFactory.GetDefaultOptions();

            if (port.HasValue)
                opts.Url = $"nats://localhost:{port.Value}";

            return opts;
        }

        public Options GetTestOptions(int? port = null, Action<Options> optionsModifier = null)
        {
            var opts = GetTestOptionsWithDefaultTimeout(port);
            opts.Timeout = 10000;
            optionsModifier?.Invoke(opts);

            return opts;
        }

        public IConnection OpenConnection(int? port = null, Action<Options> optionsModifier = null)
        {
            var opts = GetTestOptions(port, optionsModifier);

            return ConnectionFactory.CreateConnection(opts);
        }

        public IEncodedConnection OpenEncodedConnectionWithDefaultTimeout(int? port = null)
        {
            var opts = GetTestOptionsWithDefaultTimeout(port);

            return ConnectionFactory.CreateEncodedConnection(opts);
        }

        public void RunInServer(TestServerInfo testServerInfo, Action<IConnection> test)
        {
            using (var s = NATSServer.CreateFastAndVerify(testServerInfo.Port))
            {
                using (var c = OpenConnection(testServerInfo.Port))
                {
                    test(c);
                }
            }
        }

        public void RunInJsServer(TestServerInfo testServerInfo, Action<IConnection> test)
        {
            using (var s = NATSServer.CreateJetStreamFastAndVerify(testServerInfo.Port))
            {
                using (var c = OpenConnection(testServerInfo.Port))
                {
                    try
                    {
                        test(c);
                    }
                    finally
                    {
                        cleanupJs(c);
                    }
                }
            }
        }

        public void RunInJsServer(TestServerInfo testServerInfo, Action<Options> optionsModifier, Action<IConnection> test)
        {
            using (var s = NATSServer.CreateJetStreamFastAndVerify(testServerInfo.Port, optionsModifier))
            {
                using (var c = OpenConnection(testServerInfo.Port, optionsModifier))
                {
                    try
                    {
                        test(c);
                    }
                    finally
                    {
                        cleanupJs(c);
                    }
                }
            }
        }

        private void cleanupJs(IConnection c)
        {
            IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
            IList<string> streams = jsm.GetStreamNames();
            foreach (string s in streams)
            {
                jsm.DeleteStream(s);
            }
        }
    }

    public class DefaultSuiteContext : SuiteContext
    {
        public const string CollectionKey = "9733f463316047fa9207e0a3aaa3c41a";

        private const int SeedPortNormalServers = TestSeedPorts.DefaultSuiteNormalServers;

        public readonly TestServerInfo DefaultServer = new TestServerInfo(Defaults.Port);

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPortNormalServers);
        public readonly TestServerInfo Server2 = new TestServerInfo(SeedPortNormalServers + 1);
        public readonly TestServerInfo Server3 = new TestServerInfo(SeedPortNormalServers + 2);
        public readonly TestServerInfo Server4 = new TestServerInfo(SeedPortNormalServers + 3);
        public readonly TestServerInfo Server5 = new TestServerInfo(SeedPortNormalServers + 4);
        public readonly TestServerInfo Server6 = new TestServerInfo(SeedPortNormalServers + 5);
        public readonly TestServerInfo Server7 = new TestServerInfo(SeedPortNormalServers + 6);

        private const int SeedPortClusterServers = TestSeedPorts.DefaultSuiteNormalClusterServers;

        public readonly TestServerInfo ClusterServer1 = new TestServerInfo(SeedPortClusterServers);
        public readonly TestServerInfo ClusterServer2 = new TestServerInfo(SeedPortClusterServers + 1);
        public readonly TestServerInfo ClusterServer3 = new TestServerInfo(SeedPortClusterServers + 2);
        public readonly TestServerInfo ClusterServer4 = new TestServerInfo(SeedPortClusterServers + 3);
        public readonly TestServerInfo ClusterServer5 = new TestServerInfo(SeedPortClusterServers + 4);
        public readonly TestServerInfo ClusterServer6 = new TestServerInfo(SeedPortClusterServers + 5);
        public readonly TestServerInfo ClusterServer7 = new TestServerInfo(SeedPortClusterServers + 6);
    }

    public class AuthorizationSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.AuthorizationSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
        public readonly TestServerInfo Server2 = new TestServerInfo(SeedPort + 1);
        public readonly TestServerInfo Server3 = new TestServerInfo(SeedPort + 2);
    }

    public class ConnectionSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.ConnectionSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
        public readonly TestServerInfo Server2 = new TestServerInfo(SeedPort + 1);
    }

    public class ConnectionSecuritySuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.ConnectionSecuritySuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
    }

    public class ConnectionDrainSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.ConnectionDrainSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
    }

    public class ConnectionIpV6SuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.ConnectionIpV6Suite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
    }

    public class ConnectionMemoryLeaksSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.ConnectionMemoryLeaksSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
        public readonly TestServerInfo Server2 = new TestServerInfo(SeedPort + 1);
        public readonly TestServerInfo Server3 = new TestServerInfo(SeedPort + 2);
    }

    public class ClusterSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.ClusterSuite;

        public readonly TestServerInfo ClusterServer1 = new TestServerInfo(SeedPort);
        public readonly TestServerInfo ClusterServer2 = new TestServerInfo(SeedPort + 1);
        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort + 2);
        public readonly TestServerInfo Server2 = new TestServerInfo(SeedPort + 3);
        public readonly TestServerInfo Server3 = new TestServerInfo(SeedPort + 4);
        public readonly TestServerInfo Server4 = new TestServerInfo(SeedPort + 5);
        public readonly TestServerInfo Server5 = new TestServerInfo(SeedPort + 6);
        public readonly TestServerInfo Server6 = new TestServerInfo(SeedPort + 7);
        public readonly TestServerInfo Server7 = new TestServerInfo(SeedPort + 8);
        public readonly TestServerInfo Server8 = new TestServerInfo(SeedPort + 9);

        public readonly TestServerInfo[] TestServers;
        public readonly TestServerInfo[] TestServersShortList;

        public ClusterSuiteContext()
        {
            TestServers = new[]
            {
                Server1,
                Server2,
                Server3,
                Server4,
                Server5,
                Server6,
                Server7,
                Server8
            };

            TestServersShortList = TestServers.Take(2).ToArray();
        }

        public string[] GetTestServersUrls() => TestServers.Select(s => s.Url).ToArray();

        public string[] GetTestServersShortListUrls() => TestServersShortList.Select(s => s.Url).ToArray();
    }

    public class AsyncAwaitDeadlocksSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.AsyncAwaitDeadlocksSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
    }

    public class EncodingSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.EncodingSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
    }

    public class ReconnectSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.ReconnectSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
    }

    public class PublishErrorsDuringReconnectSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.PublishErrorsDuringReconnectSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
    }

    public class RxSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.RxSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
    }

    public class SubscriptionsSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.SubscriptionsSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
    }

    public class TlsSuiteContext : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.TlsSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
        public readonly TestServerInfo Server2 = new TestServerInfo(SeedPort + 1);
        public readonly TestServerInfo Server3 = new TestServerInfo(SeedPort + 2);
    }

    public class JetStreamSuiteContext : OneServerSuiteContext {}
    public class JetStreamManagementSuiteContext : OneServerSuiteContext {}
    public class JetStreamPublishSuiteContext : OneServerSuiteContext {}
    public class JetStreamPushAsyncSuiteContext : OneServerSuiteContext {}
    public class JetStreamPushSyncSuiteContext : OneServerSuiteContext {}
    public class JetStreamPushSyncQueueSuiteContext : OneServerSuiteContext {}
    public class JetStreamPullSuiteContext : OneServerSuiteContext {}
    public class KeyValueSuiteContext : OneServerSuiteContext {}
    
    public class OneServerSuiteContext : SuiteContext
    {
        public readonly TestServerInfo Server1;
            
        public OneServerSuiteContext()
        {
            Server1 = new TestServerInfo(TestSeedPorts.AutoPort.Increment());
        }
        
        public void RunInJsServer(Action<IConnection> test) => base.RunInJsServer(Server1, test);
        public void RunInServer(Action<IConnection> test) => base.RunInServer(Server1, test);
    }

    public sealed class SkipPlatformsWithoutSignals : FactAttribute
    {
        public SkipPlatformsWithoutSignals()
        {
            if (NATSServer.SupportsSignals == false)
            {
                Skip = "Ignore environments that do not support signaling.";
            }
        }
    }
}