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
using System.Threading.Tasks;
using NATS.Client.Internals;
using NATS.Client.Service.Contexts;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class Service
    {
        private readonly IConnection conn;
        private readonly string id;
        private readonly StatsDataDecoder statsDataDecoder;
        private readonly int drainTimeoutMillis;

        public InfoResponse InfoResponse { get; }
        public SchemaResponse SchemaResponse { get; }

        private readonly Context serviceContext;
        private readonly IList<Context> discoveryContexts;

        private readonly Object stopLock;
        private TaskCompletionSource<bool> doneTcs;

        internal Service(ServiceBuilder builder)
        {
            id = new Nuid().GetNext();
            conn = builder.Conn;
            statsDataDecoder = builder.StatsDataDecoder;
            drainTimeoutMillis = builder.DrainTimeoutMillis;

            InfoResponse = new InfoResponse(id, builder.Name, builder.Version, builder.Description, builder.Subject);
            SchemaResponse = new SchemaResponse(id, builder.Name, builder.Version, builder.SchemaRequest, builder.SchemaResponse);

            // do the service first in case the server feels like rejecting the subject
            StatsResponse statsResponse = new StatsResponse(id, builder.Name, builder.Version);
            serviceContext = new ServiceContext(conn, InfoResponse.Subject, statsResponse, builder.ServiceMessageHandler); 

            discoveryContexts = new List<Context>();
            AddDiscoveryContexts(ServiceUtil.Ping, new PingResponse(id, InfoResponse.Name, InfoResponse.Version).Serialize());
            AddDiscoveryContexts(ServiceUtil.Info, InfoResponse.Serialize());
            AddDiscoveryContexts(ServiceUtil.Schema, SchemaResponse.Serialize());
            AddStatsContexts(statsResponse, builder.StatsDataSupplier);

            stopLock = new object();
        }

        public Task<bool> StartService()
        {
            serviceContext.Start();
            foreach (var ctx in discoveryContexts)
            {
                ctx.Start();
            }

            doneTcs = new TaskCompletionSource<bool>();
            return doneTcs.Task;
        }
        
        private void AddDiscoveryContexts(string action, byte[] response) {
            discoveryContexts.Add(new DiscoveryContext(conn, action, null, null, response));
            discoveryContexts.Add(new DiscoveryContext(conn, action, InfoResponse.Name, null, response));
            discoveryContexts.Add(new DiscoveryContext(conn, action, InfoResponse.Name, id, response));
        }

        private void AddStatsContexts(StatsResponse statsResponse, StatsDataSupplier sds) {
            discoveryContexts.Add(new StatsContext(conn, null, null, statsResponse, sds));
            discoveryContexts.Add(new StatsContext(conn, InfoResponse.Name, null, statsResponse, sds));
            discoveryContexts.Add(new StatsContext(conn, InfoResponse.Name, id, statsResponse, sds));
        }

        public void Stop(bool drain = true, Exception e = null) {
            lock (stopLock) {
                if (!doneTcs.Task.IsCompleted) {
                    if (drain)
                    {
                        List<Task> tasks = new List<Task>();

                        foreach (var c in discoveryContexts) {
                            tasks.Add(c.Sub.DrainAsync(drainTimeoutMillis));
                        }

                        // make sure drain is done before closing dispatcher
                        foreach (var t in tasks)
                        {
                            try {
                                t.Wait(drainTimeoutMillis);
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

        public void Reset() {
            serviceContext.StatsResponse.Reset();
        }

        public string Id => InfoResponse.ServiceId;

        public StatsResponse StatsResponse => serviceContext.StatsResponse.Copy(statsDataDecoder);
  
        public override string ToString()
        {
            return $"Service: {InfoResponse}";
        }
    }
 }
 