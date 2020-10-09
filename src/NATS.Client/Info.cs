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

using System.Linq;
using System.Text;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client
{
    internal class ServerInfo
    {
        public string server_id { get; private set; }

        public string client_ip { get; private set; }

        public string host { get; private set; }

        public int port { get; private set; }

        public string version { get; private set; }

        public bool auth_required { get; private set; }

        public bool tls_required { get; private set; }

        public bool headers { get; private set; }

        public long max_payload { get; private set; }

        public string[] connect_urls { get; private set; }

        public string nonce { get; private set; }

        public int proto { get; private set; }

        public bool ldm { get; private set; }

        public static ServerInfo CreateFromJson(string json)
        {
            var x = JSON.Parse(json);

            return new ServerInfo
            {
                server_id = x["server_id"].Value,
                client_ip = x["client_ip"].Value,
                host = x["host"].Value,
                port = x["port"].AsInt,
                version = x["version"].Value,
                auth_required = x["auth_required"].AsBool,
                tls_required = x["tls_required"].AsBool,
                max_payload = x["max_payload"].AsLong,
                connect_urls = x["connect_urls"].Children.Select(n => n.Value).ToArray(),
                nonce = x["nonce"].Value,
                proto = x["proto"].AsInt,
                headers = x["headers"].AsBool,
                ldm = x["ldm"].AsBool,
            };
        }
    }
    
    internal class ConnectInfo
    {
        public bool verbose { get; private set; }

        public bool pedantic { get; private set; }

        public string user { get; private set; }

        public string pass { get; private set; }

        public bool ssl_required { get; private set; }

        public string name { get; private set; }

        public string auth_token { get; private set; }

        public string lang { get; private set; } = Defaults.LangString;

        public string version { get; private set; } = Defaults.Version;

        public int protocol { get; private set; } = (int) ClientProtcolVersion.ClientProtoInfo;

        public string jwt { get; private set; }

        public string nkey { get; private set; }

        public string sig { get; private set; }

        public bool echo { get; private set; }

        public bool headers { get; private set; }

        public bool no_responders { get; private set; }

        internal ConnectInfo(bool verbose, bool pedantic, string ujwt, string nkey, string sig, 
            string user, string pass, string token, bool secure, string name, bool echo)
        {
            this.verbose = verbose;
            this.pedantic = pedantic;
            this.jwt = ujwt;
            this.nkey = nkey;
            this.sig = sig;
            this.user = user;
            this.pass = pass;
            this.ssl_required = secure;
            this.name = name;
            this.auth_token = token;
            this.echo = echo;
            this.headers = true;
            this.no_responders = true;
        }

        internal StringBuilder AppendAsJsonTo(StringBuilder sb)
        {
            var n = new JSONObject
            {
                ["verbose"] = verbose,
                ["pedantic"] = pedantic,
                ["user"] = user ?? string.Empty,
                ["pass"] = pass ?? string.Empty,
                ["ssl_required"] = ssl_required,
                ["name"] = name ?? string.Empty,
                ["auth_token"] = auth_token ?? string.Empty,
                ["lang"] = lang ?? string.Empty,
                ["version"] = version ?? string.Empty,
                ["protocol"] = protocol,
                ["jwt"] = jwt ?? string.Empty,
                ["nkey"] = nkey ?? string.Empty,
                ["sig"] = sig ?? string.Empty,
                ["echo"] = echo,
                ["headers"] = headers,
                ["no_responders"] = no_responders,
            };

            n.WriteToStringBuilder(sb, 0, 0, JSONTextMode.Compact);

            return sb;
        }
    }
}
