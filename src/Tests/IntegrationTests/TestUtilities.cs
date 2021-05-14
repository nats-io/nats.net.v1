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
using System.Text;
using NATS.Client;
#if NET46
using System.Reflection;
#endif

namespace IntegrationTests
{
    public class NATSServer : IDisposable
    {
#if NET46
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
            psInfo = createProcessStartInfo();

            args = args == null
                ? $"-p {port}"
                : $"-p {port} {args}";

            addArgument(psInfo, args);

            p = Process.Start(psInfo);

            if (delay > TimeSpan.Zero)
                Thread.Sleep(delay);
        }

        public static NATSServer CreateFast(string args = null)
            => CreateFast(Defaults.Port, args);

        public static NATSServer CreateFast(int port, string args = null)
            => new NATSServer(TimeSpan.Zero, port, args);

        public static NATSServer CreateJetStreamFast(string args = null)
            => CreateJetStreamFast(Defaults.Port, args);

        public static NATSServer CreateJetStreamFast(int port, string args = null)
        {
            args = args == null
                ? $"-js"
                : $"{args} -js";

            return new NATSServer(TimeSpan.Zero, port, args); 
        }

        public static NATSServer CreateJetStreamFastAndVerify(string args = null)
            => CreateJetStreamFastAndVerify(Defaults.Port, args);

        public static NATSServer CreateJetStreamFastAndVerify(int port, string args = null)
        {
            args = args == null
                ? $"-js"
                : $"{args} -js";

            return CreateFastAndVerify(port, args);
        }

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
                    using(var c = cf.CreateConnection(opts))
                        c.Close();
                    isVerifiedOk = true;
                    break;
                }
                catch
                {
                    Thread.Sleep(i * 250);
                }
            }

            if (!isVerifiedOk)
                throw new Exception($"Could not connect to test NATS-Server at '{opts.Url}'");

            return server;
        }

        public static NATSServer Create(string args = null)
            => Create(Defaults.Port, args);

        public static NATSServer Create(int port, string args = null)
            => new NATSServer(DefaultDelay, port, args);

        public static NATSServer CreateWithConfig(int port, string configFile)
            => new NATSServer(DefaultDelay, port, $"-config {configFile}");

        private void addArgument(ProcessStartInfo psi, string arg)
        {
            if (psi.Arguments == null)
            {
                psi.Arguments = arg;
            }
            else
            {
                string args = psi.Arguments;
                args += arg;
                psi.Arguments = args;
            }
        }

        private ProcessStartInfo createProcessStartInfo()
        {
            var psi = new ProcessStartInfo(SERVEREXE);
            psi.UseShellExecute = Debug || !HideWindow;

            if (Debug)
            {
                psi.Arguments = " -DV ";
            }
            else
            {
                if (HideWindow)
                {
                    psi.CreateNoWindow = true;
                    psi.ErrorDialog = false;
                }
            }

            psi.WorkingDirectory = UnitTestUtilities.GetConfigDir();
            return psi;
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
                var s = false;

                if (p.MainWindowHandle != IntPtr.Zero)
                    s = p.CloseMainWindow();

                if (!s)
                    p.Kill();

                p.WaitForExit(250);
            }
            catch (Exception)
            {
                // ignored
            }
            finally
            {
                p.Close();
            }
        }

        public void Shutdown()
        {
            if (p == null)
                return;

            stopProcess(p);

            p = null;
        }

        public static bool SupportsSignals
        {
            get => Environment.OSVersion.Platform == PlatformID.MacOSX ||
                       Environment.OSVersion.Platform == PlatformID.Unix;
        }

        /// <summary>
        /// Sets lame duck mode if the OS supports signals.  Otherwise, we need to
        /// get adminstrative permissions, install a windows service, start it, etc.
        /// </summary>
        public void SetLameDuckMode()
        {
            if (!SupportsSignals)
                throw new NotSupportedException("LDM testing is only support on platforms with signals.");

            var ldmPs = createProcessStartInfo();
            addArgument(ldmPs, " -sl ldm=" + p.Id);
            var ldmp = Process.Start(ldmPs);
            ldmp.WaitForExit();
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
            if (!processes.Any())
                return;

            foreach (var proc in getProcesses())
                stopProcess(proc);

            // Let the OS cleanup.
            for (int i = 0; i < 10; i++)
            {
                processes = getProcesses();
                if (!processes.Any())
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
            if (!Directory.Exists(configDirPath))
                throw new DirectoryNotFoundException($"The Config dir was not found at: '{configDirPath}'.");

            return configDirPath;
        }

        public static string GetFullCertificatePath(string certificateName)
            => Path.Combine(GetConfigDir(), "certs", certificateName);
    }

    public sealed class TestSync : IDisposable
    {
        private SemaphoreSlim semaphore;
        private readonly int initialNumOfActors;

        private static readonly TimeSpan WaitTs = TimeSpan.FromMilliseconds(1000);
        private static readonly TimeSpan WaitPatientlyTs = TimeSpan.FromMilliseconds(2000);

        private TestSync(int numOfActors)
        {
            initialNumOfActors = numOfActors;
            semaphore = new SemaphoreSlim(0, numOfActors);
        }

        public static TestSync SingleActor() => new TestSync(1);

        public static TestSync TwoActors() => new TestSync(2);

        public static TestSync FourActors() => new TestSync(4);

        private void Wait(int aquireCount, TimeSpan ts)
        {
            if (Debugger.IsAttached)
            {
                ts = TimeSpan.FromMilliseconds(-1);
            }

            using (var cts = new CancellationTokenSource(ts))
            {
                for (var c = 0; c < aquireCount; c++)
                    semaphore.Wait(cts.Token);
            }
        }

        public void WaitForAll(TimeSpan? ts = null) => Wait(initialNumOfActors, ts ?? WaitTs);

        public void WaitForAllPatiently(TimeSpan? ts = null) => Wait(initialNumOfActors, ts ?? WaitPatientlyTs);
        
        public void WaitForOne(TimeSpan? ts = null) => Wait(1, ts ?? WaitTs);

        public void WaitForOnePatiently(TimeSpan? ts = null) => Wait(1, ts ?? WaitPatientlyTs);
        
        public void SignalComplete()
        {
            semaphore.Release(1);
        }

        public void Dispose()
        {
            semaphore?.Dispose();
            semaphore = null;
        }
    }

    public sealed class SamplePayload : IEquatable<SamplePayload>
    {
        private static readonly Encoding Enc = Encoding.UTF8;

        public readonly byte[] Data;
        public readonly string Text;

        private SamplePayload(string text)
        {
            Text = text ?? throw new ArgumentNullException(nameof(text));
            Data = Enc.GetBytes(text);
        }

        private SamplePayload(byte[] data)
        {
            Data = data ?? throw new ArgumentNullException(nameof(data));
            Text = Enc.GetString(data);
        }

        public static SamplePayload Random() => new SamplePayload(Guid.NewGuid().ToString("N"));

        public static implicit operator SamplePayload(Msg m) => new SamplePayload(m.Data);

        public static implicit operator byte[](SamplePayload p) => p.Data;

        public bool Equals(SamplePayload other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;

            return other.Data.SequenceEqual(Data);
        }

        public override bool Equals(object obj) => Equals(obj as SamplePayload);

        public override int GetHashCode() => Data.GetHashCode();

        public override string ToString() => Text;

        public static bool operator ==(SamplePayload left, SamplePayload right) => Equals(left, right);

        public static bool operator !=(SamplePayload left, SamplePayload right) => !Equals(left, right);
    }
}
