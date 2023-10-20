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
using System.Text;

namespace NATSExamples.ClientCompatibility
{
    public static class Log
    {
        private static readonly string INDENT = "    ";
        private static readonly string NEWLINE_INDENT = "\n" + INDENT;

        public static void info(string label)
        {
            log(label, null, false);
        }

        public static void error(string label)
        {
            log(label, null, true);
        }

        public static void info(string label, params Object[] extras)
        {
            log(label, false, extras);
        }

        public static void error(string label, params Object[] extras)
        {
            log(label, true, extras);
        }

        public static void error(string label, Exception e)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(e.Message);
            log(label, sb.ToString(), true);
            Console.Error.WriteLine(e);
        }

        private static void log(string label, bool error, params Object[] extras)
        {
            if (extras.Length == 1)
            {
                log(label, stringify(extras[0]), error);
            }
            else
            {
                bool notFirst = false;
                StringBuilder sb = new StringBuilder();
                foreach (Object extra in extras) {
                    string s = stringify(extra);
                    if (s != null)
                    {
                        if (notFirst)
                        {
                            sb.Append("\n");
                        }
                        else
                        {
                            notFirst = true;
                        }

                        sb.Append(s);
                    }
                }
                log(label, stringify(sb), error);
            }
        }

        private static void log(string label, string extraStr, bool error)
        {
            string start = "[" + time() + "] " + label;

            if (string.IsNullOrEmpty(extraStr))
            {
                if (error)
                {
                    Console.Error.WriteLine(start);
                }
                else
                {
                    Console.WriteLine(start);
                }

                return;
            }

            extraStr = NEWLINE_INDENT + extraStr.Replace("\n", NEWLINE_INDENT);
            if (error)
            {
                Console.Error.WriteLine(start + extraStr);
            }
            else
            {
                Console.WriteLine(start + extraStr);
            }
        }

        private static string time()
        {
            return DateTime.Now.ToLongTimeString();
        }

        private static string stringify(Object o)
        {
            if (o == null)
            {
                return null;
            }

            if (o is byte[] bytes) {
                if (bytes.Length == 0)
                {
                    return null;
                }

                return Encoding.UTF8.GetString(bytes);
            }
            string s = o.ToString().Trim();
            return string.IsNullOrEmpty(s) ? null : s;
        }
    }
}
