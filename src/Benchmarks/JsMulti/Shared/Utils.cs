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

namespace JsMulti.Shared
{
    public class Utils
    {
        public const string HdrPubTime = "pt";

        public static string UniqueEnough()
        {
            return Guid.NewGuid().ToString("N").Substring(20);
        }

        public static void Log(string label, Object s)
        {
            Console.WriteLine(Format(label, s));
        }

        public static void LogEx(string label, Exception e)
        {
            Console.Error.WriteLine(Format(label, e));
        }

        public static string Format(string label, object s)
        {
            return DateTimeOffset.Now.ToUnixTimeMilliseconds() + " [" + label + "] " + s;
        }
    }
}