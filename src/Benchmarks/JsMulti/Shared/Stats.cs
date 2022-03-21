// Copyright 2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
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
using System.Diagnostics;
using System.IO;
using JsMulti.Settings;
using JsMulti.Shared;
using NATS.Client;

namespace JsMulti
{
    public class Stats
    {
        private const double MillisPerSecond = 1000;
        private const double TicksPerMillisecond = 10000;
        private const double TicksPerSecond = TicksPerMillisecond * 1000;

        private const long HumanBytesBase = 1024;
        private static readonly string[] HumanBytesUnits = {"b", "kb", "mb", "gb", "tb", "pb", "eb"};
        private const string Zeros = "000000000";

        private const string ReportSepLine    = "| --------------- | ----------------- | --------------- | ------------------------ | ---------------- |";
        private const string ReportLineHeader = "| {0,-15} |             count |            time |                 msgs/sec |        bytes/sec |\n";
        private const string ReportLineFormat = "| {0,-15} | {1,12} msgs | {2,12} ms | {3,15} msgs/sec | {4,12}/sec |\n";

        private const string LtReportSepLine    = "| --------------- | ------------------------ | ---------------- | ------------------------ | ---------------- | ------------------------ | ---------------- |";
        private const string LtReportLineHeader = "| Latency Total   |                   Publish to Server Created |         Server Created to Consumer Received |                Publish to Consumer Received |";
        private const string LtReportLineFormat = "| {0,-15} | {1,15} msgs/sec | {2,12}/sec | {3,15} msgs/sec | {4,12}/sec | {5,15} msgs/sec | {6,12}/sec |\n";

        private const string LmReportSepLine    = "| ----------------- | ------------------- | ------------------- | ------------------- |";
        private const string LmReportLineHeader = "| Latency Message   | Publish to Server   | Server to Consumer  | Publish to Consumer |";
        private const string LmReportLineFormat = "| {0,17} |  {1,15} ms |  {2,15} ms |  {3,15} ms |\n";

        private double _elapsed = 0;
        private double _bytes = 0;
        private int _messageCount = 0;

        // latency
        private double _messagePubToServerTimeElapsed = 0;
        private double _messageServerToReceiverElapsed = 0;
        private double _messageFullElapsed = 0;
        private double _messagePubToServerTimeElapsedForAverage = 0;
        private double _messageServerToReceiverElapsedForAverage = 0;
        private double _messageFullElapsedForAverage = 0;
        private double _maxMessagePubToServerTimeElapsed = 0;
        private double _maxMessageServerToReceiverElapsed = 0;
        private double _maxMessageFullElapsed = 0;
        private double _minMessagePubToServerTimeElapsed = long.MaxValue;
        private double _minMessageServerToReceiverElapsed = long.MaxValue;
        private double _minMessageFullElapsed = long.MaxValue;

        // Time keeping
        private readonly Stopwatch _stopwatch;

        // Misc
        private string _hdrLabel { get; }

        public Stats() : this ("") {}
        
        public Stats(JsmAction action) : this(action.Label) {}

        private Stats(string hdrLabel)
        {
            _hdrLabel = hdrLabel;
            _stopwatch = new Stopwatch();
            _stopwatch.Start(); // it will be restarted
        }

        public void Start() {
            _stopwatch.Restart();
        }

        public void Stop() {
            _stopwatch.Stop();
            _elapsed += _stopwatch.ElapsedTicks;
        }

        public long Elapsed() {
            _stopwatch.Stop();
            return _stopwatch.ElapsedTicks;
        }

        public void AcceptHold(long hold) {
            if (hold > 0) {
                _elapsed += hold;
            }
        }

        public void Count(long bytes) {
            _messageCount++;
            _bytes += bytes;
        }

        public void StopAndCount(long bytes) {
            Stop();
            Count(bytes);
        }

        public void Count(Msg m, long mReceived) {
            _messageCount++;
            _bytes += m.Data.Length;
            string hPubTime = m.HasHeaders ? m.Header[Utils.HdrPubTime] : null;
            if (hPubTime != null) {
                long.TryParse(hPubTime, out var messagePubTime);
                long messageStampTime = new DateTimeOffset(m.MetaData.Timestamp).ToUnixTimeMilliseconds();
                
                double el = ElapsedLatency(messagePubTime, messageStampTime);
                _messagePubToServerTimeElapsed += el;
                _maxMessagePubToServerTimeElapsed = Math.Max(_maxMessagePubToServerTimeElapsed, el);
                _minMessagePubToServerTimeElapsed = Math.Min(_minMessagePubToServerTimeElapsed, el);

                el = ElapsedLatency(messageStampTime, mReceived);
                _messageServerToReceiverElapsed += el;
                _maxMessageServerToReceiverElapsed = Math.Max(_maxMessageServerToReceiverElapsed, el);
                _minMessageServerToReceiverElapsed = Math.Min(_minMessageServerToReceiverElapsed, el);
                
                el = ElapsedLatency(messagePubTime, mReceived);
                _messageFullElapsed += el;
                _maxMessageFullElapsed = Math.Max(_maxMessageFullElapsed, el);
                _minMessageFullElapsed = Math.Min(_minMessageFullElapsed, el);
            }
        }

        private double ElapsedLatency(double startMs, double stopMs) {
            double d = stopMs - startMs;
            return d < 1 ? 0 : d * TicksPerMillisecond;
        }

        public static void Report(Stats stats) {
            Report(stats, "Total", true, true, Console.Out);
        }

        public static void Report(Stats stats, string label, bool header, bool footer) {
            Report(stats, label, header, footer, Console.Out);
        }

        public static void Report(Stats stats, string tlabel, bool header, bool footer, TextWriter writer) {
            double elapsed = stats._elapsed / TicksPerMillisecond;
            double messagesPerSecond = stats._elapsed == 0 ? 0 : stats._messageCount * TicksPerSecond / stats._elapsed;
            double bytesPerSecond = TicksPerSecond * (stats._bytes) / (stats._elapsed);
            if (header) {
                writer.WriteLine("\n" + ReportSepLine);
                writer.Write(ReportLineHeader, stats._hdrLabel);
                writer.WriteLine(ReportSepLine);
            }
            writer.Write(ReportLineFormat, tlabel,
                Format(stats._messageCount),
                Format3(elapsed),
                Format3(messagesPerSecond),
                HumanBytes(bytesPerSecond));
            if (footer) {
                writer.WriteLine(ReportSepLine);
            }
        }

        public static void LtReport(Stats stats, string label, bool header, bool footer, TextWriter writer) {
            if (header) {
                writer.WriteLine("\n" + LtReportSepLine);
                writer.WriteLine(LtReportLineHeader);
                writer.WriteLine(LtReportSepLine);
            }

            double pubMper = stats._messagePubToServerTimeElapsed == 0 ? 0 : stats._messageCount * TicksPerSecond / stats._messagePubToServerTimeElapsed;
            double pubBper = TicksPerSecond * (stats._bytes)/(stats._messagePubToServerTimeElapsed);
            double recMper = stats._messageServerToReceiverElapsed == 0 ? 0 : stats._messageCount * TicksPerSecond / stats._messageServerToReceiverElapsed;
            double recBper = TicksPerSecond * (stats._bytes)/(stats._messageServerToReceiverElapsed);
            double totMper = stats._messageFullElapsed == 0 ? 0 : stats._messageCount * TicksPerSecond / stats._messageFullElapsed;
            double totBper = TicksPerSecond * (stats._bytes)/(stats._messageFullElapsed);

            writer.Write(LtReportLineFormat, label,
                Format3(pubMper),
                HumanBytes(pubBper),
                Format3(recMper),
                HumanBytes(recBper),
                Format3(totMper),
                HumanBytes(totBper));
            if (footer) {
                writer.WriteLine(LtReportSepLine);
            }
        }

        public static void LmReport(Stats stats, string label, bool header, bool total, TextWriter writer) {
            if (header) {
                writer.WriteLine("\n" + LmReportSepLine);
                writer.WriteLine(LmReportLineHeader);
                writer.WriteLine(LmReportSepLine);
            }

            double pubMper;
            double recMper;
            double totMper;
            if (total) {
                pubMper = stats._messagePubToServerTimeElapsedForAverage == 0 ? 0 : stats._messagePubToServerTimeElapsedForAverage / TicksPerMillisecond / stats._messageCount;
                recMper = stats._messageServerToReceiverElapsedForAverage == 0 ? 0 : stats._messageServerToReceiverElapsedForAverage / TicksPerMillisecond / stats._messageCount;
                totMper = stats._messageFullElapsedForAverage == 0 ? 0 : stats._messageFullElapsedForAverage / TicksPerMillisecond / stats._messageCount;
            }
            else {
                pubMper = stats._messagePubToServerTimeElapsed == 0 ? 0 : stats._messagePubToServerTimeElapsed / TicksPerMillisecond / stats._messageCount;
                recMper = stats._messageServerToReceiverElapsed == 0 ? 0 : stats._messageServerToReceiverElapsed / TicksPerMillisecond / stats._messageCount;
                totMper = stats._messageFullElapsed == 0 ? 0 : stats._messageFullElapsed / TicksPerMillisecond / stats._messageCount;
            }
            writer.Write(LmReportLineFormat, label + " Average", Format(pubMper), Format(recMper), Format(totMper));

            pubMper = stats._minMessagePubToServerTimeElapsed == 0 ? 0 : stats._minMessagePubToServerTimeElapsed / TicksPerMillisecond;
            recMper = stats._minMessageServerToReceiverElapsed == 0 ? 0 : stats._minMessageServerToReceiverElapsed / TicksPerMillisecond;
            totMper = stats._minMessageFullElapsed == 0 ? 0 : stats._minMessageFullElapsed / TicksPerMillisecond;
            writer.Write(LmReportLineFormat, "Minimum", Format(pubMper), Format(recMper), Format(totMper));

            pubMper = stats._maxMessagePubToServerTimeElapsed == 0 ? 0 : stats._maxMessagePubToServerTimeElapsed / TicksPerMillisecond;
            recMper = stats._maxMessageServerToReceiverElapsed == 0 ? 0 : stats._maxMessageServerToReceiverElapsed / TicksPerMillisecond;
            totMper = stats._maxMessageFullElapsed == 0 ? 0 : stats._maxMessageFullElapsed / TicksPerMillisecond;
            writer.Write(LmReportLineFormat, "Maximum", Format(pubMper), Format(recMper), Format(totMper));

            writer.WriteLine(LmReportSepLine);
        }

        public static Stats Total(IList<Stats> statList) {
            Stats total = new Stats();
            foreach (Stats stats in statList) {
                total._elapsed = Math.Max(total._elapsed, stats._elapsed);
                total._messageCount += stats._messageCount;
                total._bytes += stats._bytes;

                total._messagePubToServerTimeElapsed = Math.Max(total._messagePubToServerTimeElapsed, stats._messagePubToServerTimeElapsed);
                total._messageServerToReceiverElapsed = Math.Max(total._messageServerToReceiverElapsed, stats._messageServerToReceiverElapsed);
                total._messageFullElapsed = Math.Max(total._messageFullElapsed, stats._messageFullElapsed);

                total._messagePubToServerTimeElapsedForAverage += stats._messagePubToServerTimeElapsed;
                total._messageServerToReceiverElapsedForAverage += stats._messageServerToReceiverElapsed;
                total._messageFullElapsedForAverage += stats._messageFullElapsed;

                total._maxMessagePubToServerTimeElapsed = Math.Max(total._maxMessagePubToServerTimeElapsed, stats._maxMessagePubToServerTimeElapsed);
                total._maxMessageServerToReceiverElapsed = Math.Max(total._maxMessageServerToReceiverElapsed, stats._maxMessageServerToReceiverElapsed);
                total._maxMessageFullElapsed = Math.Max(total._maxMessageFullElapsed, stats._maxMessageFullElapsed);

                total._minMessagePubToServerTimeElapsed = Math.Min(total._minMessagePubToServerTimeElapsed, stats._minMessagePubToServerTimeElapsed);
                total._minMessageServerToReceiverElapsed = Math.Min(total._minMessageServerToReceiverElapsed, stats._minMessageServerToReceiverElapsed);
                total._minMessageFullElapsed = Math.Min(total._minMessageFullElapsed, stats._minMessageFullElapsed);
            }
            return total;
        }

        public static void Report(IList<Stats> statList) {
            Report(statList, Console.Out);
        }

        public static void Report(IList<Stats> statList, TextWriter writer) {
            Stats totalStats = Total(statList);
            for (int x = 0; x < statList.Count; x++) {
                Report(statList[x], "Thread " + (x+1), x == 0, false, writer);
            }
            writer.WriteLine(ReportSepLine);
            Report(totalStats, "Total", false, true, writer);

            if (statList[0]._messagePubToServerTimeElapsed > 0) {
                for (int x = 0; x < statList.Count; x++) {
                    LtReport(statList[x], "Thread " + (x+1), x == 0, false, writer);
                }
                writer.WriteLine(LtReportSepLine);
                LtReport(totalStats, "Total", false, true, writer);
                for (int x = 0; x < statList.Count; x++) {
                    LmReport(statList[x], "Thread " + (x+1), x == 0, false, writer);
                }
                LmReport(totalStats, "Total", false, true, writer);

            }
        }

        public static string HumanBytes(double bytes) {
            if (bytes < HumanBytesBase) {
                return $"{bytes:0.00} b";
            }
            int exp = (int) (Math.Log(bytes) / Math.Log(HumanBytesBase));
            return $"{bytes / Math.Pow(HumanBytesBase, exp):0.00} {HumanBytesUnits[exp]}";
        }

        public static string Format(double s) {
            return $"{s:N}"; 
        }

        public static string Format(long s) {
            return $"{s:N0}"; 
        }

        public static string Format3(double d) {
            if (d >= 1_000_000_000) {
                return HumanBytes(d);
            }
            string f = Format(d);
            int at = f.IndexOf('.');
            if (at == 0) {
                return f + "." + Zeros.Substring(0, 3);
            }
            return (f + Zeros).Substring(0, at + 3 + 1);
        }
    }
}