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

namespace NATS.Client.Service.Contexts
{
    internal class StatsContext : Context
    {
        private readonly StatsDataSupplier _sds;
            
        internal StatsContext(IConnection conn, 
            String serviceName, String serviceId, Stats stats, StatsDataSupplier sds) 
            : base(conn, ServiceUtil.ToDiscoverySubject(ServiceUtil.Stats, serviceName, serviceId), stats)
        {
            _sds = sds;
        }

        protected override void SubOnMessage(object sender, MsgHandlerEventArgs args)
        {
            if (_sds != null) {
                Stats.Data = _sds.Invoke();
            }
            Conn.Publish(args.Message.Reply, Stats.Serialize());
        }
    }
}