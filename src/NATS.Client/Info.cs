// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text;

namespace NATS.Client
{
    [DataContract]
    internal class ServerInfo
    {
        internal string serverId;
        internal string serverHost;
        internal int serverPort;
        internal string serverVersion;
        internal bool authRequired;
        internal bool tlsRequired;
        internal long maxPayload;
        internal string[] connectURLs;
        internal string serverNonce;
        internal int serverProto;

        [DataMember]
        public string server_id
        {
            get { return serverId; }
            set { serverId = value; }
        }

        [DataMember]
        public string host
        {
            get { return serverHost; }
            set { serverHost = value; }
        }

        [DataMember]
        public int port
        {
            get { return serverPort; }
            set { serverPort = value; }
        }

        [DataMember]
        public string version
        {
            get { return serverVersion; }
            set { serverVersion = value; }
        }

        [DataMember]
        public bool auth_required
        {
            get { return authRequired; }
            set { authRequired = value; }
        }

        [DataMember]
        public bool tls_required
        {
            get { return tlsRequired; }
            set { tlsRequired = value; }
        }

        [DataMember]
        public long max_payload
        {
            get { return maxPayload; }
            set { maxPayload = value; }
        }

        [DataMember]
        public string[] connect_urls
        {
            get { return connectURLs; }
            set { connectURLs = value; }
        }

        [DataMember]
        public string nonce
        {
            get { return serverNonce; }
            set { serverNonce = value; }
        }

        [DataMember]
        internal int proto { get => serverProto; set => serverProto = value; }

        public static ServerInfo CreateFromJson(string json)
        {
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json)))
            {
                var serializer = new DataContractJsonSerializer(typeof(ServerInfo));
                stream.Position = 0;
                return (ServerInfo)serializer.ReadObject(stream);
            }
        }
    }
    
    [DataContract]
    internal class ConnectInfo
    {
        bool isVerbose;
        bool isPedantic;
        string userJWT;
        string userNkey;
        string signature;
        string clientUser;
        string clientPass;
        bool sslRequired;
        string clientName;
        string clientLang = Defaults.LangString;
        string clientVersion = Defaults.Version;
        int protocolVersion = (int)ClientProtcolVersion.ClientProtoInfo;
        bool clientEcho;
        string authToken = null;

        [DataMember]
        public bool verbose { get => isVerbose; set => isVerbose = value; }

        [DataMember]
        public bool pedantic { get => isPedantic; set => isPedantic = value; }

        [DataMember]
        public string user { get => clientUser; set => clientUser = value; }

        [DataMember]
        public string pass { get => clientPass; set => clientPass = value; }

        [DataMember]
        public bool ssl_required { get => sslRequired; set => sslRequired = value; }

        [DataMember]
        public string name { get => clientName; set => clientName = value; }

        [DataMember]
        public string auth_token{ get => authToken; set => authToken = value; }

        [DataMember]
        public string lang { get => clientLang; set => clientLang = value; }

        [DataMember]
        public string version { get => clientVersion; set => clientVersion = value; }

        [DataMember]
        public int protocol { get => protocolVersion; set => protocolVersion = value; }

        [DataMember]
        public string jwt { get => userJWT; set => userJWT = value; }

        [DataMember]
        public string nkey { get => userNkey; set => userNkey = value; }

        [DataMember]
        public string sig { get => signature; set => signature = value; }

        [DataMember]
        public bool echo { get => clientEcho; set => clientEcho = value; }

        internal ConnectInfo(bool verbose, bool pedantic, string ujwt, string nkey, string sig, 
            string user, string pass, string token, bool secure, string name, bool echo)
        {
            isVerbose = verbose;
            isPedantic = pedantic;
            userJWT = ujwt;
            userNkey = nkey;
            signature = sig;
            clientUser = user;
            clientPass = pass;
            sslRequired = secure;
            clientName = name;
            authToken = token;
            clientEcho = echo;
        }

        internal string ToJson()
        {
            var serializer = new DataContractJsonSerializer(typeof(ConnectInfo));
            using (var stream = new MemoryStream())
            {
                serializer.WriteObject(stream, this);
                return Encoding.UTF8.GetString(stream.ToArray());
            }
        }
    }
}
