using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using static NATSExamples.Output;

namespace NATSExamples
{
    public class Monitor
    {
        const string MonitorLabel = "MONITOR";
        
        const int ReportFrequency = 5000;
        const int ShortReports = 50;

        readonly CommandLine cmd;
        readonly ChaosPublisher publisher;
        readonly IList<ConnectableConsumer> consumers;
        readonly InterlockedBoolean reportFull;

        public Monitor(CommandLine cmd, ChaosPublisher publisher, IList<ConnectableConsumer> consumers) {
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
                            Output.Debug(MonitorLabel, "si.Config " + si.Config);
                            Output.Debug(MonitorLabel, "si.ClusterInfo " + si.ClusterInfo);
                            String message = "Stream\n" + Formatted(si.Config)
                                                        + "\n" + Formatted(si.ClusterInfo);
                            Output.ControlMessage(MonitorLabel, message);
                            reportFull.Set(false);
                            if (consumers != null) {
                                foreach (ConnectableConsumer con in consumers) {
                                    con.refreshInfo();
                                }
                            }
                        }
                        if (shortReportsOwed < 1) {
                            shortReportsOwed = ShortReports;
                            if (consumers != null) {
                                foreach (ConnectableConsumer con in consumers) {
                                    conReport.Append("\n").Append(con.Label).Append(" | Last Sequence: ").Append(con.LastReceivedSequence);
                                }
                            }
                        }
                        else {
                            shortReportsOwed--;
                            if (consumers != null) {
                                foreach (ConnectableConsumer con in consumers) {
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