using System;
using System.Text;
using NATS.Client.JetStream;

namespace NATS.Client.Internals
{
    internal static class Validator
    {
        private static readonly char[] WildGt = { '*', '>'};
        private static readonly char[] WildGtDot = { '*', '>', '.'};
        private static readonly char[] WildGtDollar = {'*', '>', '$'};

        internal static string ValidateSubject(string s, bool required)
        {
            return ValidatePrintable(s, "Subject", required);
        }

        public static string ValidateReplyTo(String s, bool required) {
            return ValidatePrintableExceptWildGt(s, "Reply To", required);
        }

        public static string ValidateQueueName(String s, bool required) {
            return ValidatePrintableExceptWildDotGt(s, "Queue", required);
        }

        public static string ValidateStreamName(String s, bool required) {
            return ValidatePrintableExceptWildDotGt(s, "Stream", required);
        }

        public static string ValidateDurable(String s, bool required) {
            return ValidatePrintableExceptWildDotGt(s, "Durable", required);
        }

        public static string ValidateDurableRequired(string durable, ConsumerConfiguration cc)
        {
            if (durable != null) return ValidateDurable(durable, true);
            if (cc != null) return ValidateDurable(cc.Durable, true);

            throw new ArgumentException(
                "Durable is required and cannot contain a '.', '*' or '>' [null]");
        }

        public static string ValidatePrefixOrDomain(String s, String label, bool required) {
            return Validate(s, required, label, () => {
                if (s.StartsWith("."))
                {
                    throw new ArgumentException($"{label} cannot start with `.` [{s}]");
                }
                if (NotPrintableOrHasWildGt(s)) {
                    throw new ArgumentException($"{label} must be in the printable ASCII range and cannot include `*`, `>` [{s}]");
                }
                return s;
            });
        }

        internal static String ValidateMustMatchIfBothSupplied(String s1, String s2, String label1, String label2) {
            // s1   | s2   || result
            // ---- | ---- || --------------
            // null | null || valid, null s2
            // null | y    || valid, y s2
            // x    | null || valid, x s1
            // x    | x    || valid, x s1
            // x    | y    || invalid
            s1 = EmptyAsNull(s1);
            s2 = EmptyAsNull(s2);
            if (s1 == null) {
                return s2; // s2 can be either null or y
            }

            // x / null or x / x
            if (s2 == null || s1.Equals(s2)) {
                return s1;
            }

            throw new ArgumentException($"{label1} [{s1}] must match the {label2} [{s2}] if both are provided.");
        }

        public static string Validate(string s, bool required, string label, Func<string> check)
        {
            string preCheck = EmptyAsNull(s);
            if (preCheck == null)
            {
                if (required) {
                    throw new ArgumentException($"{label} cannot be null or empty [{s}]");
                }
                return null;
            }

            return check.Invoke();
        }

        public static String ValidateJetStreamPrefix(String s) {
            return ValidatePrintableExceptWildGtDollar(s, "Prefix", false);
        }

        public static string ValidateMaxLength(String s, int maxLength, bool required, String label) {
            return Validate(s, required, label, () =>
            {
                int len = Encoding.UTF8.GetByteCount(s);
                if (len > maxLength) {
                    throw new ArgumentException($"{label} cannot be longer than {maxLength} bytes but was {len} bytes");
                }
                return s;
            });
        }

        public static string ValidatePrintable(string s, String label, bool required)
        {
            return Validate(s, required, label, () => {
                if (NotPrintable(s)) {
                    throw new ArgumentException($"{label} must be in the printable ASCII range [{s}]");
                }
                return s;
            });
        }

        public static string ValidatePrintableExceptWildDotGt(string s, string label, bool required)
        {
            return Validate(s, required, label, () => {
                if (NotPrintableOrHasWildGtDot(s)) {
                    throw new ArgumentException($"{label} must be in the printable ASCII range and cannot include `*` or `.` [{s}]");
                }
                return s;
            });
        }

        public static string ValidatePrintableExceptWildGt(string s, string label, bool required)
        {
            return Validate(s, required, label, () => {
                if (NotPrintableOrHasWildGt(s)) {
                    throw new ArgumentException($"{label} must be in the printable ASCII range and cannot include `*`, `>` or `$` [{s}]");
                }
                return s;
            });
        }

        public static string ValidatePrintableExceptWildGtDollar(string s, string label, bool required)
        {
            return Validate(s, required, label, () => {
                if (NotPrintableOrHasWildGtDollar(s)) {
                    throw new ArgumentException($"{label} must be in the printable ASCII range and cannot include `*`, `>` or `$` [{s}]");
                }
                return s;
            });
        }

        internal static int ValidatePullBatchSize(int pullBatchSize)
        {
            if (pullBatchSize < 1 || pullBatchSize > JetStreamConstants.MaxPullSize)
            {
                throw new ArgumentException(
                    $"Pull Batch Size must be between 1 and {JetStreamConstants.MaxPullSize} inclusive [{pullBatchSize}]");
            }

            return pullBatchSize;
        }

        internal static long ValidateMaxConsumers(long max)
        {
            return ValidateGtZeroOrMinus1(max, "Max Consumers");
        }

        internal static long ValidateMaxMessages(long max)
        {
            return ValidateGtZeroOrMinus1(max, "Max Messages");
        }

        internal static long ValidateMaxBytes(long max)
        {
            return ValidateGtZeroOrMinus1(max, "Max Bytes");
        }

        internal static long ValidateMaxMessageSize(long max)
        {
            return ValidateGtZeroOrMinus1(max, "Max message size");
        }

        internal static int ValidateNumberOfReplicas(int replicas)
        {
            if (replicas < 1 || replicas > 5)
            {
                throw new ArgumentException("Replicas must be from 1 to 5 inclusive.");
            }

            return replicas;
        }

        internal static Duration ValidateDurationRequired(Duration d)
        {
            if (d == null || d.IsZero() || d.IsNegative())
            {
                throw new ArgumentException("Duration required and must be greater than 0.");
            }

            return d;
        }

        internal static Duration ValidateDurationNotRequiredGtOrEqZero(Duration d)
        {
            if (d == null)
            {
                return Duration.Zero;
            }

            if (d.IsNegative())
            {
                throw new ArgumentException("Duration must be greater than or equal to 0.");
            }

            return d;
        }

        internal static Duration ValidateDurationNotRequiredGtOrEqZero(long millis)
        {
            if (millis < 0)
            {
                throw new ArgumentException("Duration must be greater than or equal to 0.");
            }

            return Duration.OfMillis(millis);
        }
        
        internal static object ValidateNotNull(object o, string fieldName)
        {
            if (o == null)
            {
                throw new ArgumentNullException($"{fieldName} cannot be null");
            }

            return o;
        }

        internal static string ValidateNotNull(string s, string fieldName)
        {
            if (s == null)
            {
                throw new ArgumentNullException($"{fieldName} cannot be null");
            }

            return s;
        }

        internal static string ValidateNotEmpty(string s, string fieldName)
        {
            if (s != null && s.Length == 0)
            {
                throw new ArgumentException($"{fieldName} cannot be empty");
            }

            return s;
        }

        internal static long ValidateGtZeroOrMinus1(long l, string label)
        {
            if (ZeroOrLtMinus1(l))
            {
                throw new ArgumentException($"{label} must be greater than zero or -1 for unlimited");
            }

            return l;
        }

        internal static long ValidateNotNegative(long l, String label) {
            if (l < 0) 
            {
                throw new ArgumentException($"{label} cannot be negative");
            }
            return l;
        }

        // ----------------------------------------------------------------------------------------------------
        // Helpers
        // ----------------------------------------------------------------------------------------------------

        public static bool NotPrintable(String s) {
            for (int x = 0; x < s.Length; x++) {
                char c = s[x];
                if (c < 33 || c > 126) {
                    return true;
                }
            }
            return false;
        }

        public static bool NotPrintableOrHasChars(String s, char[] charsToNotHave) {
            for (int x = 0; x < s.Length; x++) {
                char c = s[x];
                if (c < 33 || c > 126) {
                    return true;
                }
                foreach (char cx in charsToNotHave) {
                    if (c == cx) {
                        return true;
                    }
                }
            }
            return false;
        }

        private static bool NotPrintableOrHasWildGt(String s) {
            return NotPrintableOrHasChars(s, WildGt);
        }

        private static bool NotPrintableOrHasWildGtDot(String s) {
            return NotPrintableOrHasChars(s, WildGtDot);
        }

        private static bool NotPrintableOrHasWildGtDollar(String s) {
            return NotPrintableOrHasChars(s, WildGtDollar);
        }

        internal static string EmptyAsNull(string s)
        {
            return string.IsNullOrWhiteSpace(s) ? null : s;
        }

        internal static bool ZeroOrLtMinus1(long l)
        {
            return l == 0 || l < -1;
        }

        internal static Duration EnsureNotNullAndNotLessThanMin(Duration provided, Duration minimum, Duration dflt)
        {
            return provided == null || provided.Nanos < minimum.Nanos ? dflt : provided;
        }

        internal static Duration EnsureDurationNotLessThanMin(long providedMillis, Duration minimum, Duration dflt)
        {
            return EnsureNotNullAndNotLessThanMin(Duration.OfMillis(providedMillis), minimum, dflt);
        }
    }
}