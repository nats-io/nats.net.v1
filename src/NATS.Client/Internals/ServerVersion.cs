// Copyright 2023 The NATS Authors
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

namespace NATS.Client.Internals
{
    internal class ServerVersion : IComparable<ServerVersion> {
        private const string NoExtra = "~";

        readonly int major;
        readonly int minor;
        readonly int patch;
        readonly string extra;

        internal ServerVersion(string v) {
            string[] split = v.Replace("v", "").Replace("-", ".").Split('.');
            if (v.StartsWith("v")) {
                split = v.Substring(1).Replace("-", ".").Split('.');
            }
            else {
                split = v.Replace("-", ".").Split('.');
            }
            int mjr;
            int mnr;
            int ptch;
            if (int.TryParse(split[0], out mjr) &&
                int.TryParse(split[1], out mnr) &&
                int.TryParse(split[2], out ptch))
            {
                major = mjr;
                minor = mnr;
                patch = ptch;
                string xtra = null;
                for (int i = 3; i < split.Length; i++)
                {
                    if (i == 3)
                    {
                        xtra = "-" + split[i];
                    }
                    else
                    {
                        //noinspection StringConcatenationInLoop
                        xtra = xtra + "." + split[i];
                    }
                }
                extra = xtra;
            }
            else
            {
                major = 0;
                minor = 0;
                patch = 0;
                extra = null;
            }
        }

        public override string ToString()
        {
            return $"{major}.{minor}.{patch}" + (extra == null ? "" : extra);
        }

        public int CompareTo(ServerVersion o)
        {
            int c = major.CompareTo(o.major);
            if (c == 0) {
                c = minor.CompareTo(o.minor);
                if (c == 0) {
                    c = patch.CompareTo(o.patch);
                    if (c == 0) {
                        if (extra == null) {
                            c = o.extra == null ? 0 : 1;
                        }
                        else if (o.extra == null) {
                            c = -1;
                        }
                        else {
                            c = extra.CompareTo(o.extra);
                        }
                    }
                }
            }
            return c;
        }

        public static bool IsNewer(string v, string than) {
            return new ServerVersion(v).CompareTo(new ServerVersion(than)) > 0;
        }

        public static bool IsSame(string v, string than) {
            return new ServerVersion(v).CompareTo(new ServerVersion(than)) == 0;
        }

        public static bool IsOlder(string v, string than) {
            return new ServerVersion(v).CompareTo(new ServerVersion(than)) < 0;
        }

        public static bool IsSameOrOlder(string v, string than) {
            return new ServerVersion(v).CompareTo(new ServerVersion(than)) <= 0;
        }

        public static bool IsSameOrNewer(string v, string than) {
            return new ServerVersion(v).CompareTo(new ServerVersion(than)) >= 0;
        }
    }
}
