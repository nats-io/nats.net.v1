using System;

namespace NATS.Client.Internals
{
    public sealed class Duration
    {
        public const long NanosPerMilli = 1_000_000L;
        public const long NanosPerSecond = 1_000_000_000L;
        public const long NanosPerMinute = NanosPerSecond * 60L;
        public const long NanosPerHour = NanosPerMinute * 60L;
        public const long NanosPerDay = NanosPerHour * 24L;

        public static readonly Duration Zero = new Duration(0L);
        public static readonly Duration One = new Duration(1L);

        /// <summary>
        /// Gets the value of the duration in nanoseconds
        /// </summary>
        public long Nanos { get; }

        /// <summary>
        /// Gets the value of the duration in milliseconds, truncating any nano portion
        /// </summary>
        public int Millis => Convert.ToInt32(Nanos / NanosPerMilli);

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
        /// Create a Duration from seconds
        /// </summary>
        public static Duration OfSeconds(long seconds)
        {
            return new Duration(seconds * NanosPerSecond);
        }

        /// <summary>
        /// Create a Duration from minutes
        /// </summary>
        public static Duration OfMinutes(long minutes)
        {
            return new Duration(minutes * NanosPerMinute);
        }

        /// <summary>
        /// Create a Duration from hours
        /// </summary>
        public static Duration OfHours(long hours)
        {
            return new Duration(hours * NanosPerHour);
        }

        /// <summary>
        /// Create a Duration from days
        /// </summary>
        public static Duration OfDays(long days)
        {
            return new Duration(days * NanosPerDay);
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

        /// <summary>
        /// Is the value positive (greater than zero)
        /// </summary>
        /// <returns>true if value is positive</returns>
        public bool IsPositive()
        {
            return Nanos > 0;
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

        public string ToDescription() {
            if (Nanos == 0) {
                return "DUR0";
            }

            long millis = Nanos / NanosPerMilli;
            if (millis < 1000)
            {
                return "DUR" + millis + "ms";
            }

            long seconds = millis / 1000;
            millis = millis - (seconds * 1000);
            long minutes = seconds / 60;
            seconds = seconds - (minutes * 60);
            long hours = minutes / 60;
            minutes = minutes - (hours * 60);

            if (hours > 0)
            {
                return $"DUR{hours}h{minutes}m{seconds}s{millis}ms";
            }
            
            if (minutes > 0)
            {
                return $"DUR{minutes}m{seconds}s{millis}ms";
            }

            return $"DUR{seconds}s{millis}ms";
        }
    }
}
