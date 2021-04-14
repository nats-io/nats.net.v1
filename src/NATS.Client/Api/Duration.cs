namespace NATS.Client.Api
{
    public class Duration
    {
        const long NANOS_PER_SECOND = 1_000_000_000L;
        const long NANOS_PER_MILLI = 1_000_000L;

        internal static Duration ZERO = new Duration(0L);

        public long Nanos { get; }

        private Duration(long nanos)
        {
            Nanos = nanos;
        }

        internal static Duration OfNanos(long nanos)
        {
            return new Duration(nanos);
        } 

        internal static Duration OfMillis(long millis)
        {
            return new Duration(millis * NANOS_PER_MILLI);
        } 

        internal static Duration OfSeconds(long seconds)
        {
            return new Duration(seconds * NANOS_PER_SECOND);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as Duration);
        }

        private bool Equals(Duration other)
        {
            return other != null && Nanos == other.Nanos;
        }

        public override int GetHashCode()
        {
            return Nanos.GetHashCode();
        }

        public override string ToString()
        {
            return Nanos.ToString();
        }
    }
}