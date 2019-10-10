using System.Globalization;

namespace NATS.Client.Internals
{
    internal static class Formatting
    {
        internal static string ToNumericString(this int v) => v.ToString(CultureInfo.InvariantCulture);
        internal static string ToNumericString(this long v) => v.ToString(CultureInfo.InvariantCulture);
    }
}