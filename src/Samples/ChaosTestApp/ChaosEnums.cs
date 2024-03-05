// Copyright 2024 The NATS Authors
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

namespace NATSExamples
{
    public enum ChaosConsumerType
    {
        Simple, Fetch, Iterate, Push
    }
    
    public enum ChaosConsumerKind
    {
        Ephemeral, Durable, Ordered
    }

    public static class ChaosEnums
    {
        public static string ToString(ChaosConsumerType consumerType)
        {
            switch (consumerType) {
                case ChaosConsumerType.Simple:  return "Simple";
                case ChaosConsumerType.Fetch:   return "Fetch";
                case ChaosConsumerType.Iterate: return "Iterate";
                case ChaosConsumerType.Push:    return "Push";
            }

            return null;
        }
        
        public static string ToString(ChaosConsumerKind consumerKind)
        {
            switch (consumerKind) {
                case ChaosConsumerKind.Durable: return "Durable";
                case ChaosConsumerKind.Ephemeral: return "Ephemeral";
                case ChaosConsumerKind.Ordered:  return "Ordered";
            }

            return null;
        }
        
        public static ChaosConsumerType ConsumerTypeInstance(string text)
        {
            string lower = text.ToLower();
            if (lower.Equals("simple")) return ChaosConsumerType.Simple;
            if (lower.Equals("fetch")) return ChaosConsumerType.Fetch;
            if (lower.Equals("iterate")) return ChaosConsumerType.Iterate;
            if (lower.Equals("push")) return ChaosConsumerType.Push;
            throw new ArgumentException("Invalid ConsumerType: " + text);
        }

        public static ChaosConsumerKind ConsumerKindInstance(string text)
        {
            string lower = text.ToLower();
            if (lower.Equals("ephemeral")) return ChaosConsumerKind.Ephemeral;
            if (lower.Equals("durable")) return ChaosConsumerKind.Durable;
            if (lower.Equals("ordered")) return ChaosConsumerKind.Ordered;
            throw new ArgumentException("Invalid ConsumerKind: " + text);
        }
    }
}
