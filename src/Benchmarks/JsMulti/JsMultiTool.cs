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
using System.Threading;
using System.Threading.Tasks;
using JsMulti.Settings;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using static JsMulti.Settings.JsmAction;
using static JsMulti.Shared.Utils;

namespace JsMulti
{
    public abstract class JsMultiTool
    {
        public static void Main(string[] args)
        {
            Run(new Context(args), false, true);
        }

        public static IList<Stats> Run(string[] args) {
            return Run(new Context(args), false, true);
        }

        public static IList<Stats> Run(string[] args, bool printArgs, bool reportWhenDone)
        {
            return Run(new Context(args), printArgs, reportWhenDone);
        }

        public static IList<Stats> Run(Arguments args) 
        {
            return Run(new Context(args), false, true);
        }

        public static IList<Stats> Run(Arguments args, bool printArgs, bool reportWhenDone) {
            return Run(new Context(args), printArgs, reportWhenDone);
        }

        public static IList<Stats> Run(Context ctx)
        {
            return Run(ctx, false, true);
        }
        
        public static IList<Stats> Run(Context ctx, bool printArgs, bool reportWhenDone)
        {
            if (printArgs)
            {
                Console.Out.WriteLine(ctx);
            }

            Runner runner = GetRunner(ctx);
            IList<Stats> statsList = ctx.ConnShared ? RunShared(ctx, runner) : RunIndividual(ctx, runner);

            if (reportWhenDone)
            {
                Stats.Report(statsList);
            }

            return statsList;
        }

        private static Runner GetRunner(Context ctx)
        {
            try {
                switch (ctx.Action.Label) {
                    case PubSyncLabel:
                        return (c, stats, id) => PubSync(ctx,c, stats, id);
                    case PubAsyncLabel:
                        return (c, stats, id) => PubAsync(ctx,c, stats, id);
                    case PubCoreLabel:
                        return (c, stats, id) => PubCore(ctx,c, stats, id);
                    case SubPushLabel:
                        return (c, stats, id) => SubPush(ctx,c, stats, id);
                    case SubPullLabel:
                        return (c, stats, id) => SubPull(ctx,c, stats, id);
                    case SubQueueLabel:
                        if (ctx.Threads > 1) {
                            return (c, stats, id) => SubPush(ctx,c, stats, id);
                        }
                        break;
                    case SubPullQueueLabel:
                        if (ctx.Threads > 1) {
                            return (c, stats, id) => SubPull(ctx,c, stats, id);
                        }
                        break;
                }
                throw new Exception("Invalid Action");
            }
            catch (Exception e) {
                Console.WriteLine(e);
                Environment.Exit(-1);
                return null;
            }
        }

        // ----------------------------------------------------------------------------------------------------
        // Publish
        // ----------------------------------------------------------------------------------------------------
        delegate T Publisher<T>(string subject, byte[] payload);

        private static Msg BuildLatencyMessage(string subject, byte[] p)
        {
            return new Msg(subject, p)
            {
                Header =
                {
                    [HdrPubTime] = "" + DateTimeOffset.Now.ToUnixTimeMilliseconds()
                }
            };
        }

        private static void PubSync(Context ctx, IConnection c, Stats stats, int id)
        {
            IJetStream js = c.CreateJetStreamContext();
            if (ctx.LatencyFlag) {
                _pub(ctx, stats, id, (s, p) => js.Publish(BuildLatencyMessage(s, p)));
            }
            else {
                _pub(ctx, stats, id, (s, p) => js.Publish(s, p));
            }
        }

        private static void PubCore(Context ctx, IConnection c, Stats stats, int id) {
            if (ctx.LatencyFlag)
            {
                _pub(ctx, stats, id, (s, p) => { c.Publish(BuildLatencyMessage(s, p)); return null; });
            }
            else
            {
                _pub(ctx, stats, id, (s, p) => { c.Publish(s, p); return null; });
            }
        }

        private static void _pub(Context ctx, Stats stats, int id, Publisher<PublishAck> p)
        {
            string label = ctx.GetLabel(id);
            int retriesAvailable = ctx.MaxPubRetries;
            int pubTarget = ctx.GetPubCount(id);
            int published = 0;
            int unReported = 0;
            while (published < pubTarget) {
                Jitter(ctx);
                byte[] payload = ctx.GetPayload();
                stats.Start();
                try {
                    p.Invoke(ctx.Subject, payload);
                    stats.StopAndCount(ctx.PayloadSize);
                    unReported = ReportMaybe(label, ctx, ++published, ++unReported, "Published");
                }
                catch (NATSTimeoutException)
                {
                    if (--retriesAvailable == 0)
                    {
                        throw; 
                    }
                }
            }
            Report(label, published, "Completed Publishing");
        }

        private static void PubAsync(Context ctx, IConnection c, Stats stats, int id) {
            IJetStream js = c.CreateJetStreamContext(ctx.GetJetStreamOptions());
            Publisher<Task<PublishAck>> publisher;
            if (ctx.LatencyFlag) {
                publisher = (s, p) => js.PublishAsync(BuildLatencyMessage(s, p));
            }
            else
            {
                publisher = js.PublishAsync;
            }

            string label = ctx.GetLabel(id);
            List<Task<PublishAck>> futures = new List<Task<PublishAck>>();
            int roundCount = 0;
            int pubTarget = ctx.GetPubCount(id);
            int published = 0;
            int unReported = 0;
            while (published < pubTarget) {
                if (++roundCount >= ctx.RoundSize) {
                    ProcessFutures(futures, stats);
                    roundCount = 0;
                }
                Jitter(ctx);
                byte[] payload = ctx.GetPayload();
                stats.Start();
                futures.Add(publisher.Invoke(ctx.Subject, payload));
                stats.StopAndCount(ctx.PayloadSize);
                unReported = ReportMaybe(label, ctx, ++published, ++unReported, "Published");
            }
            Report(label, published, "Completed Publishing");
        }

        private static void ProcessFutures(List<Task<PublishAck>> futures, Stats stats) {
            stats.Start();
            while (futures.Count > 0)
            {
                Task<PublishAck> f = futures[0];
                futures.RemoveAt(0);
                if (!f.IsCompleted) {
                    futures.Add(f);
                }
            }
            stats.Stop();
        }

        // ----------------------------------------------------------------------------------------------------
        // Push
        // ----------------------------------------------------------------------------------------------------
        private static readonly object QueueLock = new object();
        
        private static void SubPush(Context ctx, IConnection c, Stats stats, int id)
        {
            IJetStream js = c.CreateJetStreamContext(ctx.GetJetStreamOptions());
            IJetStreamPushSyncSubscription sub;
            if (ctx.Action.IsQueue)
            {
                // if we don't do this, multiple threads will try to make the same consumer because
                // when they start, the consumer does not exist. So force them do it one at a time.
                lock (QueueLock)
                {
                    sub = js.PushSubscribeSync(ctx.Subject, ctx.QueueName,
                        ConsumerConfiguration.Builder()
                            .WithAckPolicy(ctx.AckPolicy)
                            .WithAckWait(Duration.OfSeconds(ctx.AckWaitSeconds))
                            .WithDeliverGroup(ctx.QueueName)
                                .BuildPushSubscribeOptions());
                }
                
            }
            else
            {
                sub = js.PushSubscribeSync(ctx.Subject,
                    ConsumerConfiguration.Builder()
                        .WithAckPolicy(ctx.AckPolicy)
                        .WithAckWait(Duration.OfSeconds(ctx.AckWaitSeconds))
                            .BuildPushSubscribeOptions());
            }
            
            int rcvd = 0;
            Msg lastUnAcked = null;
            int unAckedCount = 0;
            int unReported = 0;
            string label = ctx.GetLabel(id);
            InterlockedLong counter = ctx.GetSubscribeCounter(ctx.GetSubDurable(id));
            while (counter.Read() < ctx.MessageCount)
            {
                try
                {
                    stats.Start();
                    Msg m = sub.NextMessage(1000);
                    long hold = stats.Elapsed();
                    long received = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                    stats.AcceptHold(hold);
                    stats.Count(m, received);
                    counter.Increment();
                    if ( (lastUnAcked = AckMaybe(ctx, stats, m, ++unAckedCount)) == null ) {
                        unAckedCount = 0;
                    }
                    unReported = ReportMaybe(label, ctx, ++rcvd, ++unReported, "Messages Read");
                }
                catch (NATSTimeoutException)
                {
                    // normal timeout
                    long hold = stats.Elapsed();
                    AcceptHoldOnceStarted(label, stats, rcvd, hold);
                }
            }
            if (lastUnAcked != null) {
                _ack(stats, lastUnAcked);
            }
            Report(label, rcvd, "Finished Reading Messages");
        }

        // ----------------------------------------------------------------------------------------------------
        // Pull
        // ----------------------------------------------------------------------------------------------------
        private static void SubPull(Context ctx, IConnection c, Stats stats, int id)
        {
            IJetStream js = c.CreateJetStreamContext(ctx.GetJetStreamOptions());
            
            // Really only need to lock when queueing b/c it's the same durable...
            // To ensure protection from multiple threads trying  make the same consumer because
            string durable = ctx.GetSubDurable(id);
            IJetStreamPullSubscription sub;
            lock (QueueLock)
            {
                sub = js.PullSubscribe(ctx.Subject,
                    ConsumerConfiguration.Builder()
                        .WithAckPolicy(ctx.AckPolicy)
                        .WithAckWait(ctx.AckWaitSeconds)
                        .WithDurable(durable)
                        .BuildPullSubscribeOptions()
                );
            }

            _subPullFetch(ctx, stats, sub, id, durable);
        }
        
        private static void _subPullFetch(Context ctx, Stats stats, IJetStreamPullSubscription sub, int id, string counterKey) {
            int rcvd = 0;
            Msg lastUnAcked = null;
            int unAckedCount = 0;
            int unReported = 0;
            string label = ctx.GetLabel(id);
            InterlockedLong counter = ctx.GetSubscribeCounter(counterKey);
            while (counter.Read() < ctx.MessageCount)
            {
                stats.Start();
                IList<Msg> list = sub.Fetch(ctx.BatchSize, 500);
                long hold = stats.Elapsed();
                long received = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                int lc = list.Count;
                if (lc > 0)
                {
                    foreach (Msg m in list)
                    {
                        stats.Count(m, received);
                        counter.Increment();
                        if ((lastUnAcked = AckMaybe(ctx, stats, m, ++unAckedCount)) == null)
                        {
                            unAckedCount = 0;
                        }
                    }
                    rcvd += lc;
                    unReported = ReportMaybe(label, ctx, rcvd, unReported + lc, "Messages Read");
                }
                AcceptHoldOnceStarted(label, stats, rcvd, hold);
            }
            if (lastUnAcked != null) {
                _ack(stats, lastUnAcked);
            }
            Report(label, rcvd, "Finished Reading Messages");
        }

        // ----------------------------------------------------------------------------------------------------
        // Helpers
        // ----------------------------------------------------------------------------------------------------
        private static void AcceptHoldOnceStarted(string label, Stats stats, int rcvd, long hold) {
            if (rcvd == 0) {
                Log(label, "Waiting for first message.");
            }
            else {
                // not the first message so we count waiting time
                stats.AcceptHold(hold);
            }
        }

        // This method returns null if message is acked or policy is None
        private static Msg AckMaybe(Context ctx, Stats stats, Msg m, int unAckedCount) {

            if (ctx.AckPolicy == AckPolicy.Explicit) {
                _ack(stats, m);
                return null;
            }
            if (ctx.AckPolicy == AckPolicy.All) {
                if (unAckedCount >= ctx.AckAllFrequency) {
                    _ack(stats, m);
                    return null;
                }
                return m;
            }
            // AckPolicy.None
            return null;
        }

        private static void _ack(Stats stats, Msg m) {
            stats.Start();
            m.Ack();
            stats.Stop();
        }

        private static void Report(string label, int total, string message) {
            Log(label, message + " " + Stats.Format(total));
        }

        private static int ReportMaybe(string label, Context ctx, int total, int unReported, string message) {
            if (unReported >= ctx.ReportFrequency) {
                Report(label, total, message);
                return 0; // there are 0 unreported now
            }
            return unReported;
        }

        private static readonly Random Rand = new Random();
        private static void Jitter(Context ctx) {
            if (ctx.Jitter > 0) {
                Thread.Sleep(Rand.Next(0, ctx.Jitter));
            }
        }

        private static IConnection Connect(Context ctx)
        {
            IConnection c = new ConnectionFactory().CreateConnection(ctx.GetOptions());
            for (long x = 0; x < 100; x++) { // waits up to 10 seconds (100 * 100 = 10000) millis to be connected
                Thread.Sleep(100);
                if (c.State == ConnState.CONNECTED) {
                    return c;
                }
            }
            return c;
        }

        // ----------------------------------------------------------------------------------------------------
        // Runners
        // ----------------------------------------------------------------------------------------------------
        delegate void Runner(IConnection c, Stats stats, int id);

        private static IList<Stats> RunShared(Context ctx, Runner runner) {
            IList<Stats> statsList = new List<Stats>();
            using (IConnection c = Connect(ctx)) {
                List<Thread> threads = new List<Thread>();
                for (int x = 0; x < ctx.Threads; x++) {
                    int id = x + 1;
                    Stats stats = new Stats(ctx.Action);
                    statsList.Add(stats);
                    Thread t = new Thread(() => {
                        try {
                            runner.Invoke(c, stats, id);
                        } catch (Exception e) {
                            LogEx(ctx.GetLabel(id), e);
                        }
                    });
                    threads.Add(t);
                }
                foreach (Thread t in threads) { t.Start(); }
                foreach (Thread t in threads) { t.Join(); }
            }
            
            return statsList;
        }

        private static IList<Stats> RunIndividual(Context ctx, Runner runner) {
            IList<Stats> statsList = new List<Stats>();
            List<Thread> threads = new List<Thread>();

            for (int x = 0; x < ctx.Threads; x++) {
                int id = x + 1;
                Stats stats = new Stats(ctx.Action);
                statsList.Add(stats);
                Thread t = new Thread(() => {
                    using (IConnection c = Connect(ctx)) {
                        try {
                            runner.Invoke(c, stats, id);
                        } catch (Exception e) {
                            LogEx(ctx.GetLabel(id), e);
                        }
                    }
                });
                threads.Add(t);
            }
            foreach (Thread t in threads) { t.Start(); }
            foreach (Thread t in threads) { t.Join(); }

            return statsList;
        }
    }
}