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

namespace NATS.Client.JetStream
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
            ServerId = siNode[ApiConstants.ServerId].Value;
            ServerName = siNode[ApiConstants.ServerName].Value;
            Version = siNode[ApiConstants.Version].Value;
            GoVersion = siNode[ApiConstants.Go].Value;
            Host = siNode[ApiConstants.Host].Value;
            HeadersSupported = siNode[ApiConstants.Headers].AsBool;
            AuthRequired = siNode[ApiConstants.AuthRequired].AsBool;
            Nonce = siNode[ApiConstants.Nonce].Value;
            TlsRequired = siNode[ApiConstants.Tls].AsBool;
            LameDuckMode = siNode[ApiConstants.LameDuckMode].AsBool;
            JetStreamAvailable = siNode[ApiConstants.Jetstream].AsBool;
            Port = siNode[ApiConstants.Port].AsInt;
            ProtocolVersion = siNode[ApiConstants.Proto].AsInt;
            MaxPayload = siNode[ApiConstants.MaxPayload].AsLong;
            ClientId = siNode[ApiConstants.ClientId].AsInt;
            ClientIp = siNode[ApiConstants.ClientIp].Value;
            Cluster = siNode[ApiConstants.Cluster].Value;
            ConnectURLs = siNode[ApiConstants.ConnectUrls].Children
                .Where(n => !string.IsNullOrWhiteSpace(n.Value))
                .Select(n => n.Value)
                .ToArray();
        }
    }
}
