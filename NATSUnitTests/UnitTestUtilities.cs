// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Reflection;
using System.IO;
using Xunit;

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
            p = Process.Start(psInfo);
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

            p = Process.Start(psInfo);
        }

        public NATSServer(string args)
        {
            ProcessStartInfo psInfo = createProcessStartInfo();
            addArgument(psInfo, args);
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

            psInfo.WorkingDirectory = UnitTestUtilities.GetConfigDir();

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

    class ConditionalObj
    {
        Object objLock = new Object();
        bool completed = false;

        internal void wait(int timeout)
        {
            lock (objLock)
            {
                if (completed)
                    return;

                Assert.True(Monitor.Wait(objLock, timeout));
            }
        }

        internal void reset()
        {
            lock (objLock)
            {
                completed = false;
            }
        }

        internal void notify()
        {
            lock (objLock)
            {
                completed = true;
                Monitor.Pulse(objLock);
            }
        }
    }

    class UnitTestUtilities
    {
        Object mu = new Object();
        static NATSServer defaultServer = null;
        Process authServerProcess = null;

        internal static string GetConfigDir()
        {
            var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().CodeBase);
            var codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
            var runningDirectory = Path.GetDirectoryName(codeBasePath);
            return Path.Combine(runningDirectory, "config");
        }

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
                try
                {
                    defaultServer.Shutdown();
                }
                catch (Exception) { }

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
        
        internal NATSServer CreateServerOnPort(int p)
        {
            return new NATSServer(p);
        }

        internal NATSServer CreateServerWithConfig(string configFile)
        {
            return new NATSServer(" -config " + configFile);
        }

        internal NATSServer CreateServerWithArgs(string args)
        {
            return new NATSServer(" " + args);
        }

        internal static String GetFullCertificatePath(string certificateName)
        {
            return GetConfigDir() + "\\certs\\" + certificateName;
        }

        internal static void CleanupExistingServers()
        {
            try
            {
                Process[] procs = Process.GetProcessesByName("gnatsd");

                foreach (Process proc in procs)
                {
                    proc.Kill();
                }
            }
            catch (Exception) { } // ignore
        }
    }
}
