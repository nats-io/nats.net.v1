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
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
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
        private readonly Dictionary<string, EndpointContext> serviceContexts;
        private readonly IList<EndpointContext> discoveryContexts;

        public PingResponse PingResponse { get; }
        public InfoResponse InfoResponse { get; }
        public SchemaResponse SchemaResponse { get; }

        private readonly Object stopLock;
        private TaskCompletionSource<bool> doneTcs;
        private DateTime started;

        internal Service(ServiceBuilder b)
        {
            string id = new Nuid().GetNext();
            conn = b.Conn;
            DrainTimeoutMillis = b.DrainTimeoutMillis;
            stopLock = new object();

            // set up the service contexts
            // ! also while we are here, we need to collect the endpoints for the SchemaResponse
            IList<string> infoSubjects = new List<string>();
            IList<EndpointResponse> schemaEndpoints = new List<EndpointResponse>();
            serviceContexts = new Dictionary<string, EndpointContext>();
            foreach (ServiceEndpoint se in b.ServiceEndpoints.Values)
            {
                serviceContexts[se.Name] = new EndpointContext(conn, true, se);
                infoSubjects.Add(se.Subject);
                schemaEndpoints.Add(new EndpointResponse(se.Name, se.Subject, se.Endpoint.Schema));
            }

            // build static responses
            PingResponse = new PingResponse(id, b.Name, b.Version, b.Metadata);
            InfoResponse = new InfoResponse(id, b.Name, b.Version, b.Metadata, b.Description, infoSubjects);
            SchemaResponse = new SchemaResponse(id, b.Name, b.Version, b.Metadata, b.ApiUrl, schemaEndpoints);

            discoveryContexts = new List<EndpointContext>();
            AddDiscoveryContexts(SrvPing, PingResponse);
            AddDiscoveryContexts(SrvInfo, InfoResponse);
            AddDiscoveryContexts(SrvSchema, SchemaResponse);
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
            return new Endpoint(subject, subject, null, null, false);
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

        public Task<bool> StartService()
        {
            doneTcs = new TaskCompletionSource<bool>();
            foreach (var ctx in serviceContexts.Values)
            {
                ctx.Start();
            }
            foreach (var ctx in discoveryContexts)
            {
                ctx.Start();
            }
            started = DateTime.UtcNow;
            return doneTcs.Task;
        }

        public static ServiceBuilder Builder() {
            return new ServiceBuilder();
        }

        public void Stop(Exception e)
        {
            Stop(true, e);
        }

        public void Stop(bool drain = true, Exception e = null) {
            lock (stopLock) {
                if (!doneTcs.Task.IsCompleted) {
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
                        doneTcs.SetResult(true);
                    }
                    else {
                        doneTcs.SetException(e);
                    }
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
 
        public string Id => InfoResponse.Id;
        public string Name => InfoResponse.Name;
        public string Version => InfoResponse.Version;
        public string Description => InfoResponse.Description;
        public string ApiUrl => SchemaResponse.ApiUrl;

        public StatsResponse GetStatsResponse()
        {
            IList<EndpointResponse> endpointStatsList = new List<EndpointResponse>();
            foreach (EndpointContext c in serviceContexts.Values)
            {
                endpointStatsList.Add(c.GetEndpointStats());
            }
            return new StatsResponse(PingResponse, started, endpointStatsList);
        }
        
        public EndpointResponse GetEndpointStats(string endpointName)
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
            JsonUtils.AddField(o, ApiConstants.ApiUrl, SchemaResponse.ApiUrl);
            JSONArray ja = new JSONArray();
            foreach (EndpointResponse e in SchemaResponse.Endpoints)
            {
                ja.Add(e.ToJsonNode());
            }
            o[ApiConstants.Endpoints] = ja;
            return "\"Service\":" + o.ToString();
        }
    }
 }
 