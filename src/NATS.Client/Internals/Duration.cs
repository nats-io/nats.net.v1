namespace NATS.Client.Internals
{
    public sealed class Duration
    {
        const long NanosPerSecond = 1_000_000_000L;
        const long NanosPerMilli = 1_000_000L;

        public static readonly Duration Zero = new Duration(0L);

        /// <summary>
        /// Gets the value of the duration in nanoseconds
        /// </summary>
        public long Nanos { get; }

        /// <summary>
        /// Gets the value of the duration in milliseconds, truncating any nano portion
        /// </summary>
        public long Millis { get => Nanos / NanosPerMilli; }

        private Duration(long nanos)
        {
            Nanos = nanos;
        }

        /// <summary>
        /// Create a Duration from nanoseconds
        /// </summary>
        public static Duration OfNanos(long nanos)
        {
            return new Duration(nanos);
        } 

        /// <summary>
        /// Create a Duration from milliseconds
        /// </summary>
        public static Duration OfMillis(long millis)
        {
            return new Duration(millis * NanosPerMilli);
        } 

        /// <summary>
        /// Create a Duration from a seconds
        /// </summary>
        public static Duration OfSeconds(long seconds)
        {
            return new Duration(seconds * NanosPerSecond);
        }

        /// <summary>
        /// Is the value equal to 0
        /// </summary>
        /// <returns>true if value is 0</returns>
        public bool IsZero()
        {
            return Nanos == 0;
        }

        /// <summary>
        /// Is the value negative (less than zero)
        /// </summary>
        /// <returns>true if value is negative</returns>
        public bool IsNegative()
        {
            return Nanos < 0;
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