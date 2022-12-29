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
using System.Diagnostics;
using NATS.Client.Internals;

namespace NATS.Client.Service.Contexts
{
    internal abstract class Context
    {
        protected readonly IConnection Conn;
        internal StatsResponse StatsResponse { get; }
        internal IAsyncSubscription Sub { get; private set; }
        private readonly string subject;
        private readonly bool recordStats;
        private readonly String qGroup;

        protected Context(IConnection conn, string subject, StatsResponse statsResponse = null, bool isServiceContext = false)
        {
            Conn = conn;
            this.subject = subject;
            StatsResponse = statsResponse;
            if (isServiceContext)
            {
                qGroup = ServiceUtil.QGroup;
                recordStats = true;
            }
        }

        internal void Start()
        {
            Sub = Conn.SubscribeAsync(subject, qGroup, OnMessage);
        }
        
        protected abstract void SubOnMessage(object sender, MsgHandlerEventArgs args);

        internal void OnMessage(object sender, MsgHandlerEventArgs args)
        {
            Msg msg = args.Message;
            long requestNo = recordStats ? StatsResponse.IncrementNumRequests() : -1;
            Stopwatch sw = new Stopwatch();
            try
            {
                sw.Start();
                SubOnMessage(sender, args);
            }
            catch (Exception e)
            {
                if (recordStats)
                {
                    StatsResponse.IncrementNumErrors();
                    StatsResponse.LastError = e.ToString();
                }
                try
                {
                    ServiceMessage.ReplyStandardError(Conn, msg, e.Message, 500);
                }
                catch (Exception) { /* ignored */ }
            }
            finally
            {
                sw.Stop();
                if (recordStats)
                {
                    long total = StatsResponse.AddTotalProcessingTime(sw.ElapsedMilliseconds * Duration.NanosPerMilli);
                    StatsResponse.SetAverageProcessingTime(total / requestNo);
                }
            }
        }
    }
}