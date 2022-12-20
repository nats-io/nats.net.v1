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

namespace NATS.Client.Service
{
    public abstract class ServiceUtil
    {
        public const string Ping = "PING";
        public const string Info = "INFO";
        public const string Schema = "SCHEMA";
        public const string Stats = "STATS";
        public const string DefaultServicePrefix = "$SRV.";
        public const string QGroup = "q";
        
        public const int DefaultDrainTimeoutMillis = 5000;
        public const int DefaultDiscoveryMaxTimeMillis = 5000;
        public const int DefaultDiscoveryMaxResults = 10;

        internal static string ToDiscoverySubject(string discoverySubject, string serviceName, string serviceId)
        {
            if (string.IsNullOrEmpty(serviceId))
            {
                if (string.IsNullOrEmpty(serviceName))
                {
                    return ServiceUtil.DefaultServicePrefix + discoverySubject;
                }

                return ServiceUtil.DefaultServicePrefix + discoverySubject + "." + serviceName;
            }

            return ServiceUtil.DefaultServicePrefix + discoverySubject + "." + serviceName + "." + serviceId;
        }
    }
}