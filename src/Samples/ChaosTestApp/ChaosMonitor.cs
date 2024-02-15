using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using static NATSExamples.ChaosOutput;

namespace NATSExamples
{
    public class ChaosMonitor
    {
        const string MonitorLabel = "MONITOR";
        
        const int ReportFrequency = 5000;
        const int ShortReports = 50;

        readonly ChaosCommandLine cmd;
        readonly ChaosPublisher publisher;
        readonly IList<ChaosConnectableConsumer> consumers;
        readonly InterlockedBoolean reportFull;

        public ChaosMonitor(ChaosCommandLine cmd, ChaosPublisher publisher, IList<ChaosConnectableConsumer> consumers) {
            this.cmd = cmd;
            this.publisher = publisher;
            this.consumers = consumers;
            reportFull = new InterlockedBoolean(true);
        }

        public void Run()
        {
            Options opts = cmd.MakeOptions(() => MonitorLabel, () => { reportFull.Set(true); });
            long started = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            int shortReportsOwed = 0;
            using (IConnection conn = new ConnectionFactory().CreateConnection(opts))
            {
                IJetStreamManagement jsm = conn.CreateJetStreamManagementContext();
                while (true)
                {
                    Thread.Sleep(ReportFrequency);
                    try
                    {
                        StringBuilder conReport = new StringBuilder();
                        if (reportFull.IsTrue()) {
                            StreamInfo si = jsm.GetStreamInfo(cmd.Stream);
                            ChaosOutput.Debug(MonitorLabel, "si.Config " + si.Config);
                            ChaosOutput.Debug(MonitorLabel, "si.ClusterInfo " + si.ClusterInfo);
                            String message = "Stream\n" + Formatted(si.Config)
                                                        + "\n" + Formatted(si.ClusterInfo);
                            ChaosOutput.ControlMessage(MonitorLabel, message);
                            reportFull.Set(false);
                            if (consumers != null) {
                                foreach (ChaosConnectableConsumer con in consumers) {
                                    con.refreshInfo();
                                }
                            }
                        }
                        if (shortReportsOwed < 1) {
                            shortReportsOwed = ShortReports;
                            if (consumers != null) {
                                foreach (ChaosConnectableConsumer con in consumers) {
                                    conReport.Append("\n").Append(con.Label).Append(" | Last Sequence: ").Append(con.LastReceivedSequence);
                                }
                            }
                        }
                        else {
                            shortReportsOwed--;
                            if (consumers != null) {
                                foreach (ChaosConnectableConsumer con in consumers) {
                                    conReport.Append(" | ")
                                        .Append(con.Name)
                                        .Append(": ")
                                        .Append(con.LastReceivedSequence);
                                }
                            }
                        }

                        String pubReport = "";
                        if (publisher != null) {
                            pubReport = " | Publisher: " + publisher.LastSeqno() +
                                        (publisher.IsInErrorState() ? " (Paused)" : " (Running)");
                        }
                        ControlMessage(MonitorLabel, "Uptime: " + uptime(started) + pubReport + conReport);

                    }
                    catch (Exception e)
                    {
                        ControlMessage(MonitorLabel, e.Message + "\n" + e.StackTrace);
                        reportFull.Set(true);
                    }
                }
            }
        }
        
        private static String uptime(long started) {
            return Duration.OfMillis(DateTimeOffset.Now.ToUnixTimeMilliseconds() - started).ToDescription().Replace("DUR", "");
        }
    }
}