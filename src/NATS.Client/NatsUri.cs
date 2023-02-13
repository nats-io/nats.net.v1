// Copyright 2015-2022 The NATS Authors
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
using System.Collections.ObjectModel;

namespace NATS.Client
{
    public class NatsUri
    {
        internal const int NoPort = -1;
        internal const string UnableToParse = "Unable to parse URI string.";
        internal const string UnsupportedScheme = "Unsupported NATS URI scheme.";
        internal readonly static string[] UfeAllowsTryPrefix = new[] { "The URI scheme is not valid", "The format of the URI could not be determined", "A Dos path must be rooted" };

        public const string NATS_PROTOCOL = "nats";
        public const string TLS_PROTOCOL = "tls";
        public const string NatsProtocolSlashSlash = "nats://";
        public const int DefaultPort = 4222;

        public readonly static ReadOnlyCollection<string> KnownProtocols = new ReadOnlyCollection<string>(new []{NATS_PROTOCOL, TLS_PROTOCOL});
        public readonly static ReadOnlyCollection<string> SecureProtocols = new ReadOnlyCollection<string>(new []{TLS_PROTOCOL});

        public Uri Uri { get; }

        public string Scheme => Uri.Scheme;

        public string Host => Uri.Host;

        public int Port => Uri.Port;

        public string UserInfo => Uri.UserInfo;

        public bool Secure => SecureProtocols.Contains(Uri.Scheme);

        public bool HostIsIpAddress => Uri.HostNameType.Equals(UriHostNameType.IPv4) ||
                                       Uri.HostNameType.Equals(UriHostNameType.IPv6);
        
        public NatsUri ReHost(string newHost) {
            return new NatsUri(Uri.ToString().Replace(Uri.Host, newHost));
        }

        public override string ToString()
        {
            string s = Uri.ToString();
            if (s.EndsWith("/"))
            {
                return s.Substring(0, s.Length - 1);
            }
            return s;
        }

        protected bool Equals(NatsUri other)
        {
            return Equals(Uri, other.Uri);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj is NatsUri nuri)
            {
                obj = nuri.Uri;
            }
            return Uri.Equals(obj);
        }

        public override int GetHashCode()
        {
            return Uri.GetHashCode();
        }

        public NatsUri(Uri uri) : this(uri.ToString()) { }

        public NatsUri(string url)
        {
            /*
                test string --> result of new Uri(String)

                [1] provide protocol and try again
                1.2.3.4:4222     --> System.UriFormatException Invalid URI: The URI scheme is not valid.
                host             --> System.UriFormatException Invalid URI: The format of the URI could not be determined.
                1.2.3.4          --> System.UriFormatException Invalid URI: The format of the URI could not be determined.
                x:p@host         --> System.UriFormatException Invalid URI: A Dos path must be rooted, for example, 'c:\'.
                x:p@1.2.3.4      --> System.UriFormatException Invalid URI: A Dos path must be rooted, for example, 'c:\'.
                x:4222           --> System.UriFormatException Invalid URI: A Dos path must be rooted, for example, 'c:\'.
                x:p@host:4222    --> System.UriFormatException Invalid URI: A Dos path must be rooted, for example, 'c:\'.
                x:p@1.2.3.4:4222 --> System.UriFormatException Invalid URI: A Dos path must be rooted, for example, 'c:\'.
                
                [2] thinks it's a scheme, bbut it's not, provide protocol and try again
                host:4222 --> scheme:'host', host:'', up:'', port:-1, path:'4222'

                [3] --> throw
                proto:// --> scheme:'proto', host:'', up:'', port:-1, path:'/'
                proto://u:p@ --> System.UriFormatException Invalid URI: Invalid port specified.
                proto://:4222 xxx System.UriFormatException Invalid URI: The hostname could not be parsed.
                proto://u:p@:4222 xxx System.UriFormatException Invalid URI: The hostname could not be parsed.

                [4] has scheme and host just needs port
                proto://host        --> scheme:'proto', host:'host', up:'null', port:-1, path:'/'
                proto://u:p@host    --> scheme:'proto', host:'host', up:'u:p', port:-1, path:'/'
                proto://1.2.3.4     --> scheme:'proto', host:'1.2.3.4', up:'null', port:-1, path:'/'
                proto://u:p@1.2.3.4 --> scheme:'proto', host:'1.2.3.4', up:'u:p', port:-1, path:'/'

                [OK] has scheme, host and port
                proto://host:4222        --> scheme:'proto', host:'host', up:'', port:4222, path:'/'
                proto://u:p@host:4222    --> scheme:'proto', host:'host', up:'u:p', port:4222, path:'/'
                proto://1.2.3.4:4222     --> scheme:'proto', host:'1.2.3.4', up:'', port:4222, path:'/'
                proto://u:p@1.2.3.4:4222 --> scheme:'proto', host:'1.2.3.4', up:'u:p', port:4222, path:'/'
             */

            string workUrl = url;
            Uri workUri = null;
            
            bool IsAllowedTryPrefix(UriFormatException e)
            {
                foreach (string s in NatsUri.UfeAllowsTryPrefix)
                {
                    if (e.Message.Contains(s))
                    {
                        return true;
                    }
                }
                return false;
            }
            
            void Parse(string inUrl, bool allowTryPrefixed) {
                try {
                    workUrl = inUrl.Trim();
                    workUri = new Uri(workUrl);
                }
                catch (UriFormatException e) {
                    if (allowTryPrefixed && IsAllowedTryPrefix(e)) {
                        // [1]
                        Parse(NatsUri.NatsProtocolSlashSlash + workUrl, false);
                    }
                    else {
                        // [3]
                        throw;
                    }
                }
            }

            Parse(url, true);
            if (!workUri.LocalPath.Equals("/"))
            {
                // [2]
                Parse(NatsUri.NatsProtocolSlashSlash + workUrl, false);
            }
            
            if (string.IsNullOrEmpty(workUri.Scheme))
            {
                workUrl = NatsUri.NatsProtocolSlashSlash + workUrl;
            }
            else
            {
                string lower = workUri.Scheme.ToLower();
                if (!KnownProtocols.Contains(lower)) {
                    throw new UriFormatException(UnsupportedScheme);
                }
                if (!workUri.Scheme.Equals(lower))
                {
                    workUrl = workUrl.Replace($"{workUri.Scheme}://", $"{lower}://");
                }
            }

            if (workUri.Port == NatsUri.NoPort) {
                // [4]
                workUrl = workUrl + ":" + NatsUri.DefaultPort;
            }

            Uri = new Uri(workUrl);
        }
    }
}

