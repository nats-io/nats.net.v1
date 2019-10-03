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
using System.Threading;
using System.Diagnostics;
using System.IO;
using System.Linq;
using NATS.Client;
#if NET452
using System.Reflection;
#endif

namespace IntegrationTests
{
    public class NATSServer : IDisposable
    {
#if NET452
        static readonly string SERVEREXE = "nats-server.exe";
#else
        static readonly string SERVEREXE = "nats-server";
#endif
        static readonly TimeSpan DefaultDelay = TimeSpan.FromMilliseconds(500);

        Process p;
        ProcessStartInfo psInfo;

        // Enables debug/trace on the server
        public static bool Debug { set; get; } = false;

        // Hides the NATS server window.  Default is true;
        public static bool HideWindow { set; get; } = true;

        // Leaves the server running after dispose for debugging.
        public static bool LeaveRunning { set; get; } = false;

        static NATSServer()
        {
            cleanupExistingServers();
        }

        private NATSServer(TimeSpan delay, int port, string args = null)
        {
            createProcessStartInfo();

            args = args == null
                ? $"-p {port}"
                : $"-p {port} {args}";

            addArgument(args);

            p = Process.Start(psInfo);

            if(delay > TimeSpan.Zero)
                Thread.Sleep(delay);
        }

        public static NATSServer CreateFast(string args = null)
            => CreateFast(Defaults.Port, args);

        public static NATSServer CreateFast(int port, string args = null)
            => new NATSServer(TimeSpan.Zero, port, args);

        public static NATSServer CreateFastAndVerify(string args = null)
            => CreateFastAndVerify(Defaults.Port, args);

        public static NATSServer CreateFastAndVerify(int port, string args = null)
        {
            var server = new NATSServer(TimeSpan.Zero, port, args);
            var cf = new ConnectionFactory();
            
            var opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = $"nats://localhost:{port}";

            var isVerifiedOk = false;

            for (int i = 0; i < 10; i++)
            {
                try
                {
                    var c = cf.CreateConnection(opts);
                    c.Close();
                    isVerifiedOk = true;
                    break;
                }
                catch
                {
                    Thread.Sleep(i * 250);
                }
            }

            if(!isVerifiedOk)
                throw new Exception($"Could not connect to test NATS-Server at '{opts.Url}'");

            return server;
        }

        public static NATSServer Create(string args = null)
            => Create(Defaults.Port, args);

        public static NATSServer Create(int port, string args = null)
            => new NATSServer(DefaultDelay, port, args);

        public static NATSServer CreateWithConfig(int port, string configFile)
            => new NATSServer(DefaultDelay, port, $"-config {configFile}");

        private void addArgument(string arg)
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

        private void createProcessStartInfo()
        {
            psInfo = new ProcessStartInfo(SERVEREXE);
            psInfo.UseShellExecute = false;
            
            if (Debug)
            {
                psInfo.Arguments = " -DV ";
            }
            else
            {
                if (HideWindow)
                {
                    psInfo.CreateNoWindow = true;
                    psInfo.ErrorDialog = false;
                }
            }

            psInfo.WorkingDirectory = UnitTestUtilities.GetConfigDir();
        }

        public void Bounce(int millisDown)
        {
            Shutdown();
            Thread.Sleep(millisDown);
            p = Process.Start(psInfo);
            Thread.Sleep(500);
        }

        private static void stopProcess(Process p)
        {
            try
            {
                var successfullyClosed = p.CloseMainWindow() || p.WaitForExit(100);
                if (!successfullyClosed)
                    p.Kill();
                p.Close();
            }
            catch (Exception)
            {
                // ignored
            }
        }

        public void Shutdown()
        {
            if (p == null)
                return;

            stopProcess(p);

            p = null;
        }

        void IDisposable.Dispose()
        {
            if (LeaveRunning)
                return;

            Shutdown();
        }

        static void cleanupExistingServers()
        {
            Func<Process[]> getProcesses = () => Process.GetProcessesByName("nats-server");

            var processes = getProcesses();
            if(!processes.Any())
                return;

            foreach (var proc in getProcesses())
                stopProcess(proc);

            // Let the OS cleanup.
            for (int i = 0; i < 10; i++)
            {
                processes = getProcesses();
                if(!processes.Any())
                    break;

                Thread.Sleep(i * 250);
            }

            Thread.Sleep(250);
        }
    }

    public class UnitTestUtilities
    {
        public static string GetConfigDir()
        {
            var configDirPath = Path.Combine(Environment.CurrentDirectory, "config");
            if(!Directory.Exists(configDirPath))
                throw new DirectoryNotFoundException($"The Config dir was not found at: '{configDirPath}'.");

            return configDirPath;
        }

        public static string GetFullCertificatePath(string certificateName)
            => Path.Combine(GetConfigDir(), "certs", certificateName);
    }
}
