using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using NATS.Client.Internals;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class Discovery
    {
        private readonly IConnection conn;
        private readonly int maxTimeMillis;
        private readonly int maxResults;

        public Discovery(IConnection conn, int maxTimeMillis = -1, int maxResults = -1) 
        {
            this.conn = conn;
            this.maxTimeMillis = maxTimeMillis < 1 ? ServiceUtil.DefaultDiscoveryMaxTimeMillis : maxTimeMillis;
            this.maxResults = maxResults < 1 ? ServiceUtil.DefaultDiscoveryMaxResults : maxResults;
        }

        // ----------------------------------------------------------------------------------------------------
        // ping
        // ----------------------------------------------------------------------------------------------------
        public IList<Ping> Ping(string serviceName = null)
        {
            IList<Ping> list = new List<Ping>();
            DiscoverMany(ServiceUtil.Ping, serviceName, json => {
                list.Add(new Ping(json));
            });
            return list;
        }

        public Ping PingForNameAndId(string serviceName, string serviceId) 
        {
            string json = DiscoverOne(ServiceUtil.Ping, serviceName, serviceId);
            return json == null ? null : new Ping(json);
        }

        // ----------------------------------------------------------------------------------------------------
        // info
        // ----------------------------------------------------------------------------------------------------
        public IList<Info> Info(string serviceName = null)
        {
            IList<Info> list = new List<Info>();
            DiscoverMany(ServiceUtil.Info, serviceName, json => {
                list.Add(new Info(json));
            });
            return list;
        }

        public Info InfoForNameAndId(string serviceName, string serviceId) {
            string json = DiscoverOne(ServiceUtil.Info, serviceName, serviceId);
            return json == null ? null : new Info(json);
        }

        // ----------------------------------------------------------------------------------------------------
        // schema
        // ----------------------------------------------------------------------------------------------------
        public IList<SchemaInfo> Schema(string serviceName = null)
        {
            IList<SchemaInfo> list = new List<SchemaInfo>();
            DiscoverMany(ServiceUtil.Schema, serviceName, json => {
                list.Add(new SchemaInfo(json));
            });
            return list;
        }

        public SchemaInfo SchemaForNameAndId(string serviceName, string serviceId) 
        {
            string json = DiscoverOne(ServiceUtil.Schema, serviceName, serviceId);
            return json == null ? null : new SchemaInfo(json);
        }

        // ----------------------------------------------------------------------------------------------------
        // stats
        // ----------------------------------------------------------------------------------------------------
        public IList<Stats> Stats(string serviceName = null, StatsDataDecoder statsDataDecoder = null)
        {
            IList<Stats> list = new List<Stats>();
            DiscoverMany(ServiceUtil.Stats, serviceName, json => {
                list.Add(new Stats(json, statsDataDecoder));
            });
            return list;
        }

        public Stats StatsForNameAndId(string serviceName, string serviceId, StatsDataDecoder statsDataDecoder = null) {
            string json = DiscoverOne(ServiceUtil.Stats, serviceName, serviceId);
            return json == null ? null : new Stats(json, statsDataDecoder);
        }

        // ----------------------------------------------------------------------------------------------------
        // workers
        // ----------------------------------------------------------------------------------------------------
        private string DiscoverOne(string action, string serviceName, string serviceId) {
            try {
                string subject = ServiceUtil.ToDiscoverySubject(action, serviceName, serviceId);
                Msg m = conn.Request(subject, null, maxTimeMillis);
                return Encoding.UTF8.GetString(m.Data);
            }
            catch (NATSTimeoutException) {}
            return null;
        }

        private void DiscoverMany(string action, string serviceName, Action<string> stringConsumer) {
            ISyncSubscription sub = null;
            try {
                StringBuilder sb = new StringBuilder(Nuid.NextGlobal()).Append('-').Append(action);
                if (serviceName != null) {
                    sb.Append('-').Append(serviceName);
                }
                string replyTo = sb.ToString();

                sub = conn.SubscribeSync(replyTo);

                string subject = ServiceUtil.ToDiscoverySubject(action, serviceName, null);
                conn.Publish(subject, replyTo, null);

                int resultsLeft = maxResults;
                Stopwatch sw = Stopwatch.StartNew();
                int timeLeft = maxTimeMillis;
                while (resultsLeft > 0 && sw.ElapsedMilliseconds < maxTimeMillis) {
                    try
                    {
                        Msg msg = sub.NextMessage(timeLeft);
                        stringConsumer.Invoke(Encoding.UTF8.GetString(msg.Data));
                        resultsLeft--;
                        // try again while we have time
                        timeLeft = maxTimeMillis - (int)sw.ElapsedMilliseconds;
                    }
                    catch (NATSTimeoutException)
                    {
                        return;
                    }
                }
            }
            finally {
                try { sub?.Unsubscribe(); } catch (Exception) { /* ignored */ }
            }
        }
    }
}