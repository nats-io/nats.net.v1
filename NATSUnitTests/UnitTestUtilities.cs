// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Reflection;
using System.IO;

namespace NATSUnitTests
{
    class NATSServer : IDisposable
    {
        // Enable this for additional server debugging info.
        bool debug = false;
        Process p;

        public NATSServer()
        {
            ProcessStartInfo psInfo = createProcessStartInfo();
            this.p = Process.Start(psInfo);
            Thread.Sleep(500);
        }

        private void addArgument(ProcessStartInfo psInfo, string arg)
        {
            if (psInfo.Arguments == null)
            {
                psInfo.Arguments = arg;
            }
            else
            {
                string args = psInfo.Arguments;
                args += arg;
                psInfo.Arguments = args;
            }
        }

        public NATSServer(int port)
        {
            ProcessStartInfo psInfo = createProcessStartInfo();

            addArgument(psInfo, "-p " + port);

            this.p = Process.Start(psInfo);
        }

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

        private string buildConfigFileName(string configFile)
        {
            // TODO:  There is a better way with TestContext.
            Assembly assembly = Assembly.GetAssembly(this.GetType());
            string codebase   = assembly.CodeBase.Replace("file:///", "");
            return Path.GetDirectoryName(codebase) + "\\..\\..\\config\\" + configFile;
        }

        public NATSServer(string configFile)
        {
            ProcessStartInfo psInfo = this.createProcessStartInfo();
            addArgument(psInfo, " -config " + buildConfigFileName(configFile));
            p = Process.Start(psInfo);
        }

        private ProcessStartInfo createProcessStartInfo()
        {
            string gnatsd = Properties.Settings.Default.gnatsd;
            ProcessStartInfo psInfo = new ProcessStartInfo(gnatsd);

            if (debug)
            {
                psInfo.Arguments = " -DV ";
            }
            else
            {
                psInfo.WindowStyle = ProcessWindowStyle.Hidden;
            }

            return psInfo;
        }

        public void Shutdown()
        {
            if (p == null)
                return;

            try
            {
                p.Kill();
            }
            catch (Exception) { }

            p = null;
        }

        void IDisposable.Dispose()
        {
            Shutdown();
        }
    }

    class UnitTestUtilities
    {
        Object mu = new Object();
        static NATSServer defaultServer = null;
        Process authServerProcess = null;

        public void StartDefaultServer()
        {
            lock (mu)
            {
                if (defaultServer == null)
                {
                    defaultServer = new NATSServer();
                }
            }
        }

        public void StopDefaultServer()
        {
            lock (mu)
            {
                defaultServer.Shutdown();
                defaultServer = null;
            }
        }

        public void bounceDefaultServer(int delayMillis)
        {
            StopDefaultServer();
            Thread.Sleep(delayMillis);
            StartDefaultServer();
        }

        public void startAuthServer()
        {
            authServerProcess = Process.Start("gnatsd -config auth.conf");
        }

        internal static void testExpectedException(Action call, Type exType)
        {
            try {
               call.Invoke();
            }
            catch (Exception e)
            {
                System.Console.WriteLine(e);
                Assert.IsInstanceOfType(e, exType);
                return;
            }

            Assert.Fail("No exception thrown!");
        }

        internal NATSServer CreateServerOnPort(int p)
        {
            return new NATSServer(p);
        }

        internal NATSServer CreateServerWithConfig(string configFile)
        {
            return new NATSServer(configFile);
        }
    }
}
