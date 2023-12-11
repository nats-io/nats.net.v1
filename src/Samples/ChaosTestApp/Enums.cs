using System;

namespace NATSExamples
{
    public enum ConsumerType
    {
        Simple, Fetch, Push
    }
    
    public enum ConsumerKind
    {
        Ephemeral, Durable, Ordered
    }

    public static class Enums
    {
        public static string ToString(ConsumerType consumerType)
        {
            switch (consumerType) {
                case ConsumerType.Simple: return "Simple";
                case ConsumerType.Fetch: return "Fetch";
                case ConsumerType.Push:  return "Push";
            }

            return null;
        }
        
        public static string ToString(ConsumerKind consumerKind)
        {
            switch (consumerKind) {
                case ConsumerKind.Durable: return "Durable";
                case ConsumerKind.Ephemeral: return "Ephemeral";
                case ConsumerKind.Ordered:  return "Ordered";
            }

            return null;
        }
        
        public static ConsumerType ConsumerTypeInstance(string text)
        {
            string lower = text.ToLower();
            if (lower.Equals("simple")) return ConsumerType.Simple;
            if (lower.Equals("fetch")) return ConsumerType.Fetch;
            if (lower.Equals("push")) return ConsumerType.Push;
            throw new ArgumentException("Invalid ConsumerType: " + text);
        }

        public static ConsumerKind ConsumerKindInstance(string text)
        {
            string lower = text.ToLower();
            if (lower.Equals("ephemeral")) return ConsumerKind.Ephemeral;
            if (lower.Equals("durable")) return ConsumerKind.Durable;
            if (lower.Equals("ordered")) return ConsumerKind.Ordered;
            throw new ArgumentException("Invalid ConsumerKind: " + text);
        }
    }
}