using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace NATS.Client.Service
{
    /// <summary>
    /// Discovery is a utility class to help discover services by executing Ping, Info and Stats requests
    /// You are required to provide a connection.
    /// Optionally you can set 'maxTimeMillis' and 'maxResults'. When making a discovery request,
    /// the discovery will wait until the first one of those thresholds is reached before returning the results.
    /// <para>'maxTimeMillis' defaults to {@value DEFAULT_DISCOVERY_MAX_TIME_MILLIS}</para>
    /// <para>'maxResults' defaults tp {@value DEFAULT_DISCOVERY_MAX_RESULTS}</para>
    /// </summary>
    public class Discovery
    {
        public const int DefaultDiscoveryMaxTimeMillis = 5000;
        public const int DefaultDiscoveryMaxResults = 10;

        private readonly IConnection conn;
        private readonly int maxTimeMillis;
        private readonly int maxResults;

        private Func<string> _inboxSupplier;
        
        /// <summary>
        /// Override the normal inbox with a custom inbox to support you security model
        /// </summary>
        public Func<string> InboxSupplier
        {
            get => _inboxSupplier;
            set
            {
                _inboxSupplier = value ?? conn.NewInbox;
            }
        }

        /// <summary>
        /// Construct a Discovery instance
        /// </summary>
        /// <param name="conn">the NATS Connection</param>
        /// <param name="maxTimeMillis">optional, the maximum time to wait for discovery requests to complete or any number less than 1 to use the default</param>
        /// <param name="maxResults">optional, the maximum number of results to wait for or any number less than 1 to use the default</param>
        public Discovery(IConnection conn, int maxTimeMillis = 0, int maxResults = 0) 
        {
            this.conn = conn;
            this.maxTimeMillis = maxTimeMillis < 1 ? DefaultDiscoveryMaxTimeMillis : maxTimeMillis;
            this.maxResults = maxResults < 1 ? DefaultDiscoveryMaxResults : maxResults;
            InboxSupplier = null;
        }

        // ----------------------------------------------------------------------------------------------------
        // ping
        // ----------------------------------------------------------------------------------------------------
    
        /// <summary>
        /// Make a ping request to all services running on the server.
        /// </summary>
        /// <returns>the list of <see cref="PingResponse"/></returns>
        public IList<PingResponse> Ping()
        {
            IList<PingResponse> list = new List<PingResponse>();
            DiscoverMany(Service.SrvPing, null, json => {
                list.Add(new PingResponse(json));
            });
            return list;
        }

        /// <summary>
        /// Make a stats request only to services having the matching service name
        /// </summary>
        /// <param name="serviceName">the service name</param>
        /// <returns>the list of <see cref="StatsResponse"/></returns>
        public IList<PingResponse> Ping(string serviceName)
        {
            IList<PingResponse> list = new List<PingResponse>();
            DiscoverMany(Service.SrvPing, serviceName, json => {
                list.Add(new PingResponse(json));
            });
            return list;
        }

        /// <summary>
        /// Make a ping request only to services having the matching service name
        /// </summary>
        /// <param name="serviceName">the service name</param>
        /// <param name="serviceId">the specific service id</param>
        /// <returns>the list of <see cref="PingResponse"/></returns>
        public PingResponse PingForNameAndId(string serviceName, string serviceId) 
        {
            string json = DiscoverOne(Service.SrvPing, serviceName, serviceId);
            return json == null ? null : new PingResponse(json);
        }

        // ----------------------------------------------------------------------------------------------------
        // info
        // ----------------------------------------------------------------------------------------------------

        /// <summary>
        /// Make an info request to all services running on the server.
        /// </summary>
        /// <returns>the list of <see cref="InfoResponse"/></returns>
        public IList<InfoResponse> Info()
        {
            IList<InfoResponse> list = new List<InfoResponse>();
            DiscoverMany(Service.SrvInfo, null, json => {
                list.Add(new InfoResponse(json));
            });
            return list;
        }

        /// <summary>
        /// Make an info request only to services having the matching service name
        /// </summary>
        /// <param name="serviceName">the service name</param>
        /// <returns>the list of <see cref="InfoResponse"/></returns>
        public IList<InfoResponse> Info(string serviceName)
        {
            IList<InfoResponse> list = new List<InfoResponse>();
            DiscoverMany(Service.SrvInfo, serviceName, json => {
                list.Add(new InfoResponse(json));
            });
            return list;
        }

        /// <summary>
        /// Make an info request to a specific instance of a service having matching service name and id
        /// </summary>
        /// <param name="serviceName">the service name</param>
        /// <param name="serviceId">the specific service id</param>
        /// <returns>the list of <see cref="InfoResponse"/></returns>
        public InfoResponse InfoForNameAndId(string serviceName, string serviceId) 
        {
            string json = DiscoverOne(Service.SrvInfo, serviceName, serviceId);
            return json == null ? null : new InfoResponse(json);
        }

        // ----------------------------------------------------------------------------------------------------
        // stats
        // ----------------------------------------------------------------------------------------------------

        /// <summary>
        /// Make a stats request to all services running on the server.
        /// </summary>
        /// <returns>the list of <see cref="StatsResponse"/></returns>
        public IList<StatsResponse> Stats()
        {
            IList<StatsResponse> list = new List<StatsResponse>();
            DiscoverMany(Service.SrvStats, null, json => {
                list.Add(new StatsResponse(json));
            });
            return list;
        }

        /// <summary>
        /// Make a stats request only to services having the matching service name
        /// </summary>
        /// <param name="serviceName">the service name</param>
        /// <returns>the list of <see cref="StatsResponse"/></returns>
        public IList<StatsResponse> Stats(string serviceName)
        {
            IList<StatsResponse> list = new List<StatsResponse>();
            DiscoverMany(Service.SrvStats, serviceName, json => {
                list.Add(new StatsResponse(json));
            });
            return list;
        }

        /// <summary>
        /// Make a stats request to a specific instance of a service having matching service name and id
        /// </summary>
        /// <param name="serviceName">the service name</param>
        /// <param name="serviceId">the specific service id</param>
        /// <returns>the list of <see cref="StatsResponse"/></returns>
        public StatsResponse StatsForNameAndId(string serviceName, string serviceId) 
        {
            string json = DiscoverOne(Service.SrvStats, serviceName, serviceId);
            return json == null ? null : new StatsResponse(json);
        }

        // ----------------------------------------------------------------------------------------------------
        // workers
        // ----------------------------------------------------------------------------------------------------
        private string DiscoverOne(string action, string serviceName, string serviceId) 
        {
            try {
                string subject = Service.ToDiscoverySubject(action, serviceName, serviceId);
                Msg m = conn.Request(subject, null, maxTimeMillis);
                return Encoding.UTF8.GetString(m.Data);
            }
            catch (NATSTimeoutException) {}
            return null;
        }

        private void DiscoverMany(string action, string serviceName, Action<string> stringConsumer) 
        {
            ISyncSubscription sub = null;
            try {
                string replyTo = InboxSupplier.Invoke();

                sub = conn.SubscribeSync(replyTo);

                string subject = Service.ToDiscoverySubject(action, serviceName, null);
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