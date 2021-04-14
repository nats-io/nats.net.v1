// Copyright 2021 The NATS Authors
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

using System.Linq;
using System.Text;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.Api
{
    public sealed class ServerInfo
    {
        public string ServerId { get; }
        public string ServerName { get; }
        public string Version { get; }
        public string GoVersion { get; }
        public string Host { get; }
        public int Port { get; }
        public bool HeadersSupported { get; }
        public bool AuthRequired { get; }
        public bool TlsRequired { get; }
        public long MaxPayload { get; }
        public string[] ConnectURLs { get; }
        public int ProtocolVersion { get; }
        public string Nonce { get; }
        public bool LameDuckMode { get; }
        public bool JetStreamAvailable { get; }
        public int ClientId { get; }
        public string ClientIp { get; }
        public string Cluster { get; }

        public ServerInfo(Msg msg) : this(Encoding.UTF8.GetString(msg.Data)) { }

        public ServerInfo(string json)
        {
            var siNode = JSON.Parse(json);
            ServerId = siNode[ApiConsts.SERVER_ID].Value;
            ServerName = siNode[ApiConsts.SERVER_NAME].Value;
            Version = siNode[ApiConsts.VERSION].Value;
            GoVersion = siNode[ApiConsts.GO].Value;
            Host = siNode[ApiConsts.HOST].Value;
            HeadersSupported = siNode[ApiConsts.HEADERS].AsBool;
            AuthRequired = siNode[ApiConsts.AUTH_REQUIRED].AsBool;
            Nonce = siNode[ApiConsts.NONCE].Value;
            TlsRequired = siNode[ApiConsts.TLS].AsBool;
            LameDuckMode = siNode[ApiConsts.LAME_DUCK_MODE].AsBool;
            JetStreamAvailable = siNode[ApiConsts.JETSTREAM].AsBool;
            Port = siNode[ApiConsts.PORT].AsInt;
            ProtocolVersion = siNode[ApiConsts.PROTO].AsInt;
            MaxPayload = siNode[ApiConsts.MAX_PAYLOAD].AsLong;
            ClientId = siNode[ApiConsts.CLIENT_ID].AsInt;
            ClientIp = siNode[ApiConsts.CLIENT_IP].Value;
            Cluster = siNode[ApiConsts.CLUSTER].Value;
            ConnectURLs = siNode[ApiConsts.CONNECT_URLS].Children
                .Where(n => !string.IsNullOrEmpty(n.Value))
                .Select(n => n.Value)
                .ToArray();
        }
    }
}
