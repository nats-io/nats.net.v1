using System;

namespace NATSExamples
{
    public enum ChaosConsumerType
    {
        Simple, Fetch, Push
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
                case ChaosConsumerType.Simple: return "Simple";
                case ChaosConsumerType.Fetch: return "Fetch";
                case ChaosConsumerType.Push:  return "Push";
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