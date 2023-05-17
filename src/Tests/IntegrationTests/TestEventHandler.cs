using System;
using System.Collections.Generic;
using System.Diagnostics;
using NATS.Client;

namespace IntegrationTests
{
    public class TestEventHandler
    {
        public readonly IList<HeartbeatAlarmEventArgs> HeartbeatAlarmEvents = new List<HeartbeatAlarmEventArgs>();
        public readonly IList<StatusEventArgs> PullStatusWarningEvents = new List<StatusEventArgs>();
        public readonly IList<StatusEventArgs> PullStatusErrorEvents = new List<StatusEventArgs>();

        public void Reset()
        {
            HeartbeatAlarmEvents.Clear();
            PullStatusWarningEvents.Clear();
            PullStatusErrorEvents.Clear();
        }

        public EventHandler<HeartbeatAlarmEventArgs> HeartbeatAlarmHandler
            => (sender, e) => HeartbeatAlarmEvents.Add(e);
        
        public EventHandler<StatusEventArgs> PullStatusWarningEventHandler
            => (sender, e) => PullStatusWarningEvents.Add(e);
        
        public EventHandler<StatusEventArgs> PullStatusErrorEventHandler 
            => (sender, e) => PullStatusErrorEvents.Add(e);
        
        public Action<Options> Modifier => options =>
        {
            options.HeartbeatAlarmEventHandler = HeartbeatAlarmHandler;
            options.PullStatusWarningEventHandler = PullStatusWarningEventHandler;
            options.PullStatusErrorEventHandler = PullStatusErrorEventHandler;
        };
        
        public bool PullStatusWarningOrWait(String contains, long timeout)
        {
            Stopwatch sw = Stopwatch.StartNew();
            int i = 0;
            do {
                int count = PullStatusWarningEvents.Count;
                for (; i < count; i++) {
                    if (PullStatusWarningEvents[i].Status.Message.Contains(contains)) {
                        return true;
                    }
                }
            }
            while (sw.ElapsedMilliseconds <= timeout);
            return false;
        }
        
        public bool PullStatusErrorOrWait(String contains, long timeout)
        {
            Stopwatch sw = Stopwatch.StartNew();
            int i = 0;
            do {
                int count = PullStatusErrorEvents.Count;
                for (; i < count; i++) {
                    if (PullStatusErrorEvents[i].Status.Message.Contains(contains)) {
                        return true;
                    }
                }
            }
            while (sw.ElapsedMilliseconds <= timeout);
            return false;
        }
    }
}
