using System;
using System.Collections.Generic;
using NATS.Client;

namespace IntegrationTests
{
    public class TestEventCountHandler
    {
        public IList<HeartbeatAlarmEventArgs> HeartbeatAlarmEvents = new List<HeartbeatAlarmEventArgs>();
        public IList<StatusEventArgs> PullStatusWarningEvents = new List<StatusEventArgs>();
        public IList<StatusEventArgs> PullStatusErrorEvents = new List<StatusEventArgs>();

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
    }
}