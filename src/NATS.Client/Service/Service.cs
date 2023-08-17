// Copyright 2023 The NATS Authors
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
using System.Threading.Tasks;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// The Services Framework introduces a higher-level API for implementing services with NATS.
    /// Services automatically contain Ping, Info and Stats responders.
    /// Services have one or more service endpoints. <see cref="ServiceEndpoint"/>.
    /// When multiple instances of a service endpoints are active they work in a queue, meaning only one listener responds to any given request.
    /// </summary>
    public class Service
    {
        public const string SrvPing = "PING";
        public const string SrvInfo = "INFO";
        public const string SrvSchema = "SCHEMA";
        public const string SrvStats = "STATS";
        public const string DefaultServicePrefix = "$SRV.";

        private readonly IConnection conn;
        public int DrainTimeoutMillis { get; }
        private readonly IDictionary<string, EndpointContext> serviceContexts;
        private readonly IList<EndpointContext> discoveryContexts;

        /// <summary>
        /// The pre-constructed ping response.
        /// </summary>
        public PingResponse PingResponse { get; }
        
        /// <summary>
        /// The pre-constructed info response.
        /// </summary>
        public InfoResponse InfoResponse { get; }

        private readonly Object startStopLock;
        private TaskCompletionSource<bool> runningIndicator;
        private DateTime started;

        internal Service(ServiceBuilder b)
        {
            string id = new Nuid().GetNext();
            conn = b.Conn;
            DrainTimeoutMillis = b.DrainTimeoutMillis;
            startStopLock = new object();

            // set up the service contexts
            // ! also while we are here, we need to collect the endpoints for the SchemaResponse
            IList<Endpoint> infoEndpoints = new List<Endpoint>();
            serviceContexts = new Dictionary<string, EndpointContext>();
            foreach (ServiceEndpoint se in b.ServiceEndpoints.Values)
            {
                serviceContexts[se.Name] = new EndpointContext(conn, true, se);
                infoEndpoints.Add(se.Endpoint);
            }

            // build static responses
            PingResponse = new PingResponse(id, b.Name, b.Version, b.Metadata);
            InfoResponse = new InfoResponse(id, b.Name, b.Version, b.Metadata, b.Description, infoEndpoints);

            discoveryContexts = new List<EndpointContext>();
            AddDiscoveryContexts(SrvPing, PingResponse);
            AddDiscoveryContexts(SrvInfo, InfoResponse);
            AddStatsContexts();
        }

        private void AddDiscoveryContexts(string discoveryName, EventHandler<ServiceMsgHandlerEventArgs> handler) {
            Endpoint[] endpoints = {
                InternalEndpoint(discoveryName, null, null),
                InternalEndpoint(discoveryName, PingResponse.Name, null),
                InternalEndpoint(discoveryName, PingResponse.Name, PingResponse.Id)
            };

            foreach (Endpoint endpoint in endpoints) {
                discoveryContexts.Add(
                    new EndpointContext(conn, false,
                        new ServiceEndpoint(endpoint, handler)));
            }
        }

        private void AddDiscoveryContexts(string discoveryName, ServiceResponse sr) {
            byte[] responseBytes = sr.Serialize();
            void Handler(object sender, ServiceMsgHandlerEventArgs args) => args.Message.Respond(conn, responseBytes);
            AddDiscoveryContexts(discoveryName, Handler);
        }

        private void AddStatsContexts()
        {
            void Handler(object sender, ServiceMsgHandlerEventArgs args) => args.Message.Respond(conn, GetStatsResponse().Serialize());
            AddDiscoveryContexts(SrvStats, Handler);
        }

        private Endpoint InternalEndpoint(string discoveryName, string optionalServiceNameSegment, string optionalServiceIdSegment) {
            string subject = ToDiscoverySubject(discoveryName, optionalServiceNameSegment, optionalServiceIdSegment);
            return new Endpoint(subject, subject, null, false);
        }
 
        internal static string ToDiscoverySubject(string discoverySubject, string serviceName, string serviceId)
        {
            if (string.IsNullOrEmpty(serviceId))
            {
                if (string.IsNullOrEmpty(serviceName))
                {
                    return DefaultServicePrefix + discoverySubject;
                }

                return DefaultServicePrefix + discoverySubject + "." + serviceName;
            }

            return DefaultServicePrefix + discoverySubject + "." + serviceName + "." + serviceId;
        }

        /// <summary>
        /// Start the service
        /// </summary>
        /// <returns>a task that can be held to see if another thread called stop</returns>
        public Task<bool> StartService()
        {
            lock (startStopLock)
            {
                if (runningIndicator == null)
                {
                    runningIndicator = new TaskCompletionSource<bool>();
                    foreach (var ctx in serviceContexts.Values)
                    {
                        ctx.Start();
                    }
                    foreach (var ctx in discoveryContexts)
                    {
                        ctx.Start();
                    }
                    started = DateTime.UtcNow;
                }
                return runningIndicator.Task;
            }
        }

        /// <summary>
        /// Get an instance of a ServiceBuilder.
        /// </summary>
        /// <returns>the instance</returns>
        public static ServiceBuilder Builder()
        {
            return new ServiceBuilder();
        }

        /// <summary>
        /// Stop the service by draining.
        /// </summary>
        public void Stop() 
        {
            Stop(true, null);
        }
    
        /// <summary>
        /// Stop the service by draining. Mark the task that was received from the start method that the service had an exception.
        /// </summary>
        /// <param name="e">the error cause</param>
        public void Stop(Exception e) 
        {
            Stop(true, e);
        }
    
        /// <summary>
        /// Stop the service, optionally draining.
        /// </summary>
        /// <param name="drain">the flag indicating to drain or not</param>
        public void Stop(bool drain) 
        {
            Stop(drain, null);
        }
    
        /// <summary>
        /// Stop the service, optionally draining and optionally with an error cause
        /// </summary>
        /// <param name="drain">the flag indicating to drain or not</param>
        /// <param name="e">the optional error cause. If supplied, mark the task that was received from the start method that the service had an exception.</param>
        public void Stop(bool drain, Exception e) 
        {
            lock (startStopLock) {
                if (runningIndicator != null) {
                    if (drain)
                    {
                        List<Task> tasks = new List<Task>();

                        foreach (var c in serviceContexts.Values) {
                            tasks.Add(c.Sub.DrainAsync(DrainTimeoutMillis));
                        }

                        foreach (var c in discoveryContexts) {
                            tasks.Add(c.Sub.DrainAsync(DrainTimeoutMillis));
                        }

                        // make sure drain is done before closing dispatcher
                        foreach (var t in tasks)
                        {
                            try {
                                t.Wait(DrainTimeoutMillis);
                            }
                            catch (Exception) {
                                // don't care if it completes successfully or not, just that it's done.
                            }
                        }
                    }

                    // ok we are done
                    if (e == null) {
                        runningIndicator.SetResult(true);
                    }
                    else {
                        runningIndicator.SetException(e);
                    }
                    runningIndicator = null; // we don't need a copy anymore
                }
            }
        }

        public void Reset()
        {
            started = DateTime.UtcNow;
            foreach (EndpointContext c in discoveryContexts) {
                c.Reset();
            }
            foreach (EndpointContext c in serviceContexts.Values) {
                c.Reset();
            }
        }

        /// <value>The id of the service</value>
        public string Id => InfoResponse.Id;

        /// <value>The name of the service</value>
        public string Name => InfoResponse.Name;
        
        /// <value>The version of the service</value>
        public string Version => InfoResponse.Version;
        
        /// <value>The description of the service</value>
        public string Description => InfoResponse.Description;

        /// <summary>
        /// Get the up-to-date stats response which contains a list of all <see cref="EndpointStats"/>
        /// </summary>
        /// <returns>the stats response</returns>
        public StatsResponse GetStatsResponse()
        {
            IList<EndpointStats> endpointStatsList = new List<EndpointStats>();
            foreach (EndpointContext c in serviceContexts.Values)
            {
                endpointStatsList.Add(c.GetEndpointStats());
            }
            return new StatsResponse(PingResponse, started, endpointStatsList);
        }
        
        /// <summary>
        /// Get the up-to-date <see cref="EndpointStats"/> for a specific endpoint
        /// </summary>
        /// <param name="endpointName">the endpoint name</param>
        /// <returns>the EndpointStats or null if the name is not found</returns>
        public EndpointStats GetEndpointStats(string endpointName)
        {
            EndpointContext c;
            if (serviceContexts.TryGetValue(endpointName, out c))
            {
                return c.GetEndpointStats();
            }
            return null;
        }

        public override string ToString()
        {
            JSONObject o = new JSONObject();
            JsonUtils.AddField(o, ApiConstants.Id, InfoResponse.Id);
            JsonUtils.AddField(o, ApiConstants.Name, InfoResponse.Name);
            JsonUtils.AddField(o, ApiConstants.Version, InfoResponse.Version);
            JsonUtils.AddField(o, ApiConstants.Description, InfoResponse.Description);
            JSONArray ja = new JSONArray();
            o[ApiConstants.Endpoints] = ja;
            return "\"Service\":" + o.ToString(); // ToString() is needed because of how the JSONObject was written
        }
    }
 }
 