﻿// Copyright 2015-2023 The NATS Authors
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
using System.IO;
using System.Linq;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using UnitTests;
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

        public bool AtLeast2_9_0(IConnection c) {
            return AtLeast2_9_0(c.ServerInfo);
        }

        public bool AtLeast2_9_0(ServerInfo si) {
            return si.IsSameOrNewerThanVersion("2.9.0");
        }

        public bool AtLeast2_9_1(ServerInfo si) {
            return si.IsSameOrNewerThanVersion("2.9.1");
        }

        public bool AtLeast2_10(ServerInfo si) {
            return si.IsNewerVersionThan("2.9.99");
        }

        public bool AtLeast2_10_3(ServerInfo si) {
            return si.IsSameOrNewerThanVersion("2.10.3");
        }

        public bool AtLeast2_11(ServerInfo si) {
            return si.IsNewerVersionThan("2.10.99");
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
        public const int ConnectionBehaviorSuite = 11520; //2pc

        public static InterlockedInt AutoPort = new InterlockedInt(11550);
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

        public Options GetQuietTestOptions(int? port = null, Action<Options> optionsModifier = null)
        {
            return GetTestOptions(port, NATSServer.QuietOptionsModifier);
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
            using (var s = NATSServer.CreateFast(testServerInfo.Port))
            {
                using (var c = OpenConnection(testServerInfo.Port))
                {
                    InitRunServerInfo(c);
                    test(c);
                }
            }
        }

        public void RunInJsServer(TestServerInfo testServerInfo, Action<IConnection> test)
        {
            RunInJsServer(testServerInfo, null, null, test);
        }

        public void RunInJsServer(Func<ServerInfo, bool> versionCheck, TestServerInfo testServerInfo, Action<IConnection> test)
        {
            RunInJsServer(testServerInfo, versionCheck, null, test);
        }

        public void RunInJsServer(TestServerInfo testServerInfo, Action<Options> optionsModifier,
            Action<IConnection> test)
        {
            RunInJsServer(testServerInfo, null, optionsModifier, test);
        }

        public static ServerInfo RunServerInfo { get; private set; }

        public ServerInfo EnsureRunServerInfo() {
            if (RunServerInfo == null) {
                RunInServer(new TestServerInfo(TestSeedPorts.AutoPort.Increment()), c => {});
            }
            return RunServerInfo;
        }

        public void InitRunServerInfo(IConnection c)
        {
            if (RunServerInfo == null)
            {
                RunServerInfo = c.ServerInfo;
            }
        }

        public void RunInJsServer(TestServerInfo testServerInfo, Func<ServerInfo, bool> versionCheck, Action<Options> optionsModifier, Action<IConnection> test)
        {
            if (versionCheck != null && RunServerInfo != null) {
                if (!versionCheck(RunServerInfo)) {
                    return;
                }
                versionCheck = null; // since we've already determined it should run, null this out so we don't check below
            }

            using (var s = NATSServer.CreateJetStreamFast(testServerInfo.Port))
            {
                Action<Options> runOptionsModifier =
                    optionsModifier == null ? NATSServer.QuietOptionsModifier : optionsModifier;

                using (var c = OpenConnection(testServerInfo.Port, runOptionsModifier))
                {
                    InitRunServerInfo(c);
                    if (versionCheck != null)
                    {
                        if (!versionCheck(RunServerInfo)) {
                            return;
                        }
                    }

                    try
                    {
                        test(c);
                    }
                    finally
                    {
                        CleanupJs(c);
                    }
                }
            }
        }

        public void RunInJsServer(TestServerInfo testServerInfo, string config, Action<IConnection> test)
        {
            using (var s = NATSServer.CreateJetStreamWithConfig(testServerInfo.Port, config))
            {
                using (var c = OpenConnection(testServerInfo.Port, NATSServer.QuietOptionsModifier))
                {
                    try
                    {
                        test(c);
                    }
                    finally
                    {
                        CleanupJs(c);
                    }
                }
            }
        }

        public const string HubDomain = "HUB";
        public const string LeafDomain = "LEAF";

        public void RunInJsHubLeaf(TestServerInfo hubServerInfo,
            TestServerInfo hubLeafInfo, 
            TestServerInfo leafServerInfo, Action<IConnection, IConnection> test)
        {
            string hubConfFile = TestBase.TempConfFile();
            StreamWriter streamWriter = File.CreateText(hubConfFile);
            streamWriter.WriteLine("port: " + hubServerInfo.Port); 
            streamWriter.WriteLine("server_name: " + HubDomain);
            streamWriter.WriteLine("jetstream {");
            streamWriter.WriteLine("    store_dir: " + TestBase.TempJsStoreDir());
            streamWriter.WriteLine("    domain: " + HubDomain);
            streamWriter.WriteLine("}");
            streamWriter.WriteLine("leafnodes {");
            streamWriter.WriteLine("  listen = 127.0.0.1:" + hubLeafInfo.Port);
            streamWriter.WriteLine("}");
            streamWriter.Flush();
            streamWriter.Close();
                
            string leafConfFile = TestBase.TempConfFile();
            string leafJsStoreDir = TestBase.TempConfDir();
            streamWriter = File.CreateText(leafConfFile);
            streamWriter.WriteLine("port: " + leafServerInfo.Port); 
            streamWriter.WriteLine("server_name: " + LeafDomain);
            streamWriter.WriteLine("jetstream {");
            streamWriter.WriteLine("    store_dir: " + TestBase.TempJsStoreDir());
            streamWriter.WriteLine("    domain: " + LeafDomain);
            streamWriter.WriteLine("}");
            streamWriter.WriteLine("leafnodes {");
            streamWriter.WriteLine("  remotes = [ { url: \"leaf://127.0.0.1:" + hubLeafInfo.Port + "\" } ]");
            streamWriter.WriteLine("}");
            streamWriter.Flush();
            streamWriter.Close();
            
            using (var hub = NATSServer.CreateJetStreamFast(int.MinValue, $"--config {hubConfFile}"))
            using (var leaf = NATSServer.CreateJetStreamFast(int.MinValue, $"--config {leafConfFile}"))
            {
                using (var cHub = OpenConnection(hubServerInfo.Port, NATSServer.QuietOptionsModifier))
                using (var cLeaf = OpenConnection(leafServerInfo.Port, NATSServer.QuietOptionsModifier))
                {
                    try
                    {
                        test(cHub, cLeaf);
                    }
                    finally
                    {
                        CleanupJs(cHub);
                        CleanupJs(cLeaf);
                    }
                }
            }
        }

        private static String makeClusterConfFile(string cluster, string serverPrefix, int serverId, TestServerInfo port, TestServerInfo listen, TestServerInfo route1, TestServerInfo route2) {
            string confFile = TestBase.TempConfFile();
            StreamWriter streamWriter = File.CreateText(confFile);
            streamWriter.WriteLine("port: " + port.Port); 
            streamWriter.WriteLine("jetstream {");
            streamWriter.WriteLine("    store_dir=" + TestBase.TempJsStoreDir());
            streamWriter.WriteLine("}");
            streamWriter.WriteLine("server_name=" + serverPrefix + serverId);
            streamWriter.WriteLine("cluster {");
            streamWriter.WriteLine("  name: " + cluster);
            streamWriter.WriteLine("  listen: 127.0.0.1:" + listen.Port);
            streamWriter.WriteLine("  routes: [");
            streamWriter.WriteLine("    nats-route://127.0.0.1:" + route1.Port);
            streamWriter.WriteLine("    nats-route://127.0.0.1:" + route2.Port);
            streamWriter.WriteLine("  ]");
            streamWriter.WriteLine("}");
            streamWriter.Flush();
            streamWriter.Close();
            return confFile;
        }

        public void RunInJsCluster(Action<Options> optionsModifier, 
            TestServerInfo info1, TestServerInfo info2, TestServerInfo info3,
            TestServerInfo listen1, TestServerInfo listen2, TestServerInfo listen3, 
            Action<IConnection, IConnection, IConnection> test)
        {
            string unique = Nuid.NextGlobalSequence();
            string cluster = $"clstr{unique}";
            string serverPrefix = $"srvr{unique}";

            string confFile1 = makeClusterConfFile(cluster, serverPrefix, 1, info1, listen1, listen2, listen3);
            string confFile2 = makeClusterConfFile(cluster, serverPrefix, 2, info2, listen2, listen1, listen3);
            string confFile3 = makeClusterConfFile(cluster, serverPrefix, 3, info3, listen3, listen1, listen2);
            
            Action<Options> ClusterOptionsModifier = options =>
            {
                options.Servers = new []{ info1.Url.ToString(), info2.Url.ToString(), info3.Url.ToString() };
                NATSServer.QuietOptionsModifier.Invoke(options);
                if (optionsModifier != null)
                {
                    optionsModifier.Invoke(options);
                }
            };
            
            using (var srv1 = NATSServer.CreateJetStreamFast(int.MinValue, $"--config {confFile1}"))
            using (var srv2 = NATSServer.CreateJetStreamFast(int.MinValue, $"--config {confFile2}"))
            using (var srv3 = NATSServer.CreateJetStreamFast(int.MinValue, $"--config {confFile3}"))
            {
                using (var c1 = OpenConnection(info1.Port, ClusterOptionsModifier))
                using (var c2 = OpenConnection(info2.Port, ClusterOptionsModifier))
                using (var c3 = OpenConnection(info3.Port, ClusterOptionsModifier))
                {
                    try
                    {
                        test(c1, c2, c3);
                    }
                    finally
                    {
                        CleanupJs(c1);
                        CleanupJs(c2);
                        CleanupJs(c3);
                    }
                }
            }
        }

        public void CleanupJs(IConnection c)
        {
            try
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IList<string> streams = jsm.GetStreamNames();
                foreach (string s in streams)
                {
                    jsm.DeleteStream(s);
                }
            }
            catch (Exception)
            {
                // ignored
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

    public class ConnectionBehaviorSuite : SuiteContext
    {
        private const int SeedPort = TestSeedPorts.AuthorizationSuite;

        public readonly TestServerInfo Server1 = new TestServerInfo(SeedPort);
        public readonly TestServerInfo Server2 = new TestServerInfo(SeedPort + 1);
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
    public class KeyValueSuiteContext : JsClusterSuiteContext {}
    public class ObjectStoreSuiteContext : JsClusterSuiteContext {}
    public class ReconnectSuiteContext : JsClusterSuiteContext {}

    public class JsClusterSuiteContext : SuiteContext
    {
        public readonly TestServerInfo Server1 = new TestServerInfo(TestSeedPorts.AutoPort.Increment());
        public readonly TestServerInfo Cluster0 = new TestServerInfo(TestSeedPorts.AutoPort.Increment());
        public readonly TestServerInfo Cluster1 = new TestServerInfo(TestSeedPorts.AutoPort.Increment());
        public readonly TestServerInfo Cluster2 = new TestServerInfo(TestSeedPorts.AutoPort.Increment());
        public readonly TestServerInfo Listen0 = new TestServerInfo(TestSeedPorts.AutoPort.Increment());
        public readonly TestServerInfo Listen1 = new TestServerInfo(TestSeedPorts.AutoPort.Increment());
        public readonly TestServerInfo Listen2 = new TestServerInfo(TestSeedPorts.AutoPort.Increment());

        public void RunInJsServer(Action<IConnection> test) => base.RunInJsServer(Server1, test);
        public void RunInJsServer(Func<ServerInfo, bool> versionCheck, Action<IConnection> test) => base.RunInJsServer(Server1, versionCheck, null, test);
        public void RunInServer(Action<IConnection> test) => base.RunInServer(Server1, test);
        public void RunInJsHubLeaf(Action<IConnection, IConnection> test) => 
            base.RunInJsHubLeaf(Cluster0, Cluster1, Cluster2, test);
        public void RunInJsCluster(Action<IConnection, IConnection, IConnection> test) => 
            base.RunInJsCluster(null, Cluster0, Cluster1, Cluster2, Listen0, Listen1, Listen2, test);
        public void RunInJsCluster(Action<Options> optionsModifier, Action<IConnection, IConnection, IConnection> test) => 
            base.RunInJsCluster(optionsModifier, Cluster0, Cluster1, Cluster2, Listen0, Listen1, Listen2, test);
    }
    
    public class OneServerSuiteContext : SuiteContext
    {
        public readonly TestServerInfo Server1;
            
        public OneServerSuiteContext()
        {
            Server1 = new TestServerInfo(TestSeedPorts.AutoPort.Increment());
        }

        public void RunInServer(Action<IConnection> test) => base.RunInServer(Server1, test);
        public void RunInJsServer(Action<IConnection> test) => base.RunInJsServer(Server1, null, null, test);
        public void RunInJsServer(Func<ServerInfo, bool> versionCheck, Action<IConnection> test) => base.RunInJsServer(Server1, versionCheck, null, test);
        public void RunInJsServer(Action<Options> optionsModifier, Action<IConnection> test) => base.RunInJsServer(Server1, optionsModifier, test);
    }
    
    public class AutoServerSuiteContext : SuiteContext
    {
        public TestServerInfo AutoServer() => new TestServerInfo(TestSeedPorts.AutoPort.Increment());

        public void RunInServer(Action<IConnection> test) => base.RunInServer(AutoServer(), test);
        public void RunInJsServer(Action<IConnection> test) => base.RunInJsServer(AutoServer(), null, null, test);
        public void RunInJsServer(Func<ServerInfo, bool> versionCheck, Action<IConnection> test) => base.RunInJsServer(AutoServer(), versionCheck, null, test);
        public void RunInJsServer(Action<Options> optionsModifier, Action<IConnection> test) => base.RunInJsServer(AutoServer(), optionsModifier, test);
        public void RunInJsServer(Func<ServerInfo, bool> versionCheck, Action<Options> optionsModifier, Action<IConnection> test) => 
            base.RunInJsServer(AutoServer(), versionCheck, optionsModifier, test);
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
