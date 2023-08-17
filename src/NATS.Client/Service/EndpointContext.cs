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
using System.Diagnostics;
using NATS.Client.Internals;

namespace NATS.Client.Service
{
    /// <summary>
    /// Internal class to support service implementation
    /// </summary>
    internal class EndpointContext
    {
        private const string QGroup = "q";

        private readonly IConnection conn;
        private ServiceEndpoint se;
        private readonly bool recordStats;
        private readonly string qGroup;
        internal IAsyncSubscription Sub { get; private set; }

        private DateTime started;
        private string lastError;
        private readonly InterlockedLong numRequests;
        private readonly InterlockedLong numErrors;
        private readonly InterlockedLong processingTime;

        internal EndpointContext(IConnection conn, bool recordStats, ServiceEndpoint se)
        {
            this.conn = conn;
            this.se = se;
            this.recordStats = recordStats;
            qGroup = recordStats ? QGroup : null;

            numRequests = new InterlockedLong();
            numErrors = new InterlockedLong();
            processingTime = new InterlockedLong();
            started = DateTime.UtcNow;
        }

        internal void Start()
        {
            Sub = conn.SubscribeAsync(se.Subject, qGroup, OnMessage);
        }

        internal void OnMessage(object sender, MsgHandlerEventArgs args)
        {
            ServiceMsg smsg = new ServiceMsg(args.Message);
            Stopwatch sw = Stopwatch.StartNew();
            try
            {
                if (recordStats)
                {
                    numRequests.Increment();
                }
                se.Handler.Invoke(this, new ServiceMsgHandlerEventArgs(smsg));
            }
            catch (Exception e)
            {
                Exception b = e.GetBaseException();
                string message = b.GetType() + ": " + b.Message; 
                if (recordStats)
                {
                    numErrors.Increment();
                    lastError = message;
                }
                try
                {
                    smsg.RespondStandardError(conn, message, 500);
                }
                catch (Exception) { /* ignored */ }
            }
            finally
            {
                sw.Stop();
                if (recordStats)
                {
                    processingTime.Add(sw.ElapsedMilliseconds * Duration.NanosPerMilli);
                }
            }
        }
        
        internal EndpointStats GetEndpointStats() {
            return new EndpointStats(
                se.Endpoint.Name,
                se.Subject,
                numRequests.Read(),
                numErrors.Read(),
                processingTime.Read(),
                lastError,
                se.StatsDataSupplier?.Invoke(),
                started);
        }

        internal void Reset() {
            numRequests.Set(0);
            numErrors.Set(0);
            processingTime.Set(0);
            lastError = null;
            started = DateTime.UtcNow;
        }
    }
}