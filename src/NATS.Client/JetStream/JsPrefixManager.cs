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

using System.Collections.Concurrent;
using System.Linq;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    internal static class JsPrefixManager
    {
        private static ConcurrentDictionary<string, string> JsPrefixes = new ConcurrentDictionary<string, string>();
        
        internal static string AddPrefix(string prefix) {
            if (string.IsNullOrEmpty(prefix) || prefix.Equals(JetStreamConstants.JsapiPrefix)) {
                return JetStreamConstants.JsapiPrefix;
            }

            prefix = Validator.ValidateJetStreamPrefix(prefix);
            if (!prefix.EndsWith(".")) {
                prefix += ".";
            }

            return JsPrefixes.GetOrAdd(prefix, prefix);
        }

        internal static bool HasPrefix(string replyTo) {
            if (replyTo == null) return false;
            
            return replyTo.StartsWith(JetStreamConstants.JsapiPrefix) || 
                   JsPrefixes.Keys.Any(replyTo.StartsWith);
        }

    }
}
