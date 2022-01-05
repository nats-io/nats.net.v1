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

        public static string ValidateReplyTo(string s, bool required) {
            return ValidatePrintableExceptWildGt(s, "Reply To", required);
        }

        public static string ValidateQueueName(string s, bool required) {
            return ValidatePrintableExceptWildDotGt(s, "Queue", required);
        }

        public static string ValidateStreamName(string s, bool required) {
            return ValidatePrintableExceptWildDotGt(s, "Stream", required);
        }

        public static string ValidateDurable(string s, bool required) {
            return ValidatePrintableExceptWildDotGt(s, "Durable", required);
        }

        public static string ValidateDurableRequired(string durable, ConsumerConfiguration cc)
        {
            if (durable != null) return ValidateDurable(durable, true);
            if (cc != null) return ValidateDurable(cc.Durable, true);

            throw new ArgumentException(
                "Durable is required and cannot contain a '.', '*' or '>' [null]");
        }

        public static string ValidatePrefixOrDomain(string s, string label, bool required) {
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

        public static string ValidateKvBucketNameRequired(string s) {
            return ValidateKvBucketName(s, "Bucket name", true);
        }

        public static string ValidateKvKeyWildcardAllowedRequired(string s) {
            return ValidateWildcardKvKey(s, "Key", true);
        }

        public static string ValidateNonWildcardKvKeyRequired(string s) {
            return ValidateNonWildcardKvKey(s, "Key", true);
        }

        public static void ValidateNotSupplied(string s, ClientExDetail detail) {
            if (!string.IsNullOrWhiteSpace(s)) {
                throw detail.Instance();
            }
        }

        public static void ValidateNotSupplied(long l, long dflt, ClientExDetail detail) {
            if (l > dflt) {
                throw detail.Instance();
            }
        }

        internal static string ValidateMustMatchIfBothSupplied(string s1, string s2, ClientExDetail detail) {
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

            throw detail.Instance();
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

        public static string ValidateJetStreamPrefix(string s) {
            return ValidatePrintableExceptWildGtDollar(s, "Prefix", false);
        }

        public static string ValidateMaxLength(string s, int maxLength, bool required, string label) {
            return Validate(s, required, label, () =>
            {
                int len = Encoding.UTF8.GetByteCount(s);
                if (len > maxLength) {
                    throw new ArgumentException($"{label} cannot be longer than {maxLength} bytes but was {len} bytes");
                }
                return s;
            });
        }

        public static string ValidatePrintable(string s, string label, bool required)
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

        public static string ValidateKvBucketName(string s, string label, bool required)
        {
            return Validate(s, required, label, () => {
                if (NotRestrictedTerm(s)) {
                    throw new ArgumentException($"{label} must only contain A-Z, a-z, 0-9, `-` or `_` [{s}]");
                }
                return s;
            });
        }
        
        public static string ValidateWildcardKvKey(string s, string label, bool required)
        {
            return Validate(s, required, label, () => {
                if (NotWildcardKvKey(s)) {
                    throw new ArgumentException($"{label} must only contain A-Z, a-z, 0-9, `-`, `_`, `/`, `=` or `.` and cannot start with `.` [{s}]");
                }
                return s;
            });
        }
            
        public static string ValidateNonWildcardKvKey(string s, string label, bool required)
        {
            return Validate(s, required, label, () => {
                if (NotNonWildcardKvKey(s)) {
                    throw new ArgumentException($"{label} must only contain A-Z, a-z, 0-9, `-`, `_`, `/`, `=` or `.` and cannot start with `.` [{s}]");
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

        internal static long ValidateMaxBucketValues(long max)
        {
            return ValidateGtZeroOrMinus1(max, "Max Bucket Values"); // max bucket values is a kv alias to max messages
        }

        internal static long ValidateMaxMessagesPerSubject(long max)
        {
            return ValidateGtZeroOrMinus1(max, "Max Messages Per Subject");
        }

        internal static int ValidateMaxHistory(int max) {
            if (max < 2) {
                return 1;
            }
            if (max > JetStreamConstants.MaxHistoryPerKey) {
                throw new ArgumentException($"Max History Per Key cannot be more than {JetStreamConstants.MaxHistoryPerKey}.");
            }
            return max;
        }

        internal static long ValidateMaxHistoryPerKey(long max)
        {
            return ValidateGtZeroOrMinus1(max, "Max History Per Key");
        }
        
        internal static long ValidateMaxBytes(long max)
        {
            return ValidateGtZeroOrMinus1(max, "Max Bytes");
        }
        
        internal static long ValidateMaxBucketBytes(long max)
        {
            return ValidateGtZeroOrMinus1(max, "Max BucketBytes"); // max bucket bytes is a kv alias to max bytes
        }

        internal static long ValidateMaxMessageSize(long max)
        {
            return ValidateGtZeroOrMinus1(max, "Max Message Size");
        }

        internal static long ValidateMaxValueSize(long max)
        {
            return ValidateGtZeroOrMinus1(max, "Max Value Size"); // max value size is a kv alias to max message size
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

        internal static Duration ValidateDurationNotRequiredNotLessThanMin(Duration provided, Duration minimum)
        {
            if (provided != null && provided.Nanos < minimum.Nanos)
            {
                throw new ArgumentException("Duration must be greater than or equal to " + minimum + " nanos.");
            }

            return provided;
        }

        internal static Duration ValidateDurationNotRequiredNotLessThanMin(long millis, Duration minimum)
        {
            return ValidateDurationNotRequiredNotLessThanMin(Duration.OfMillis(millis), minimum);
        }
        
        internal static object ValidateNotNull(object o, string fieldName)
        {
            if (o == null)
            {
                throw new ArgumentNullException(fieldName);
            }

            return o;
        }

        internal static string ValidateNotNull(string s, string fieldName)
        {
            if (s == null)
            {
                throw new ArgumentNullException(fieldName);
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

        internal static long ValidateNotNegative(long l, string label) {
            if (l < 0) 
            {
                throw new ArgumentException($"{label} cannot be negative");
            }
            return l;
        }

        // ----------------------------------------------------------------------------------------------------
        // Helpers
        // ----------------------------------------------------------------------------------------------------

        public static bool NotPrintable(string s) {
            for (int x = 0; x < s.Length; x++) {
                char c = s[x];
                if (c < 33 || c > 126) {
                    return true;
                }
            }
            return false;
        }

        public static bool NotPrintableOrHasChars(string s, char[] charsToNotHave) {
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

        // restricted-term  = (A-Z, a-z, 0-9, dash 45, underscore 95)+
        public static bool NotRestrictedTerm(string s)
        {
            foreach (var c in s)
            {
                if (c < '0') { // before 0
                    if (c == '-') { // only dash is accepted
                        continue;
                    }
                    return true; // "not"
                }
                if (c < ':') {
                    continue; // means it's 0 - 9
                }
                if (c < 'A') {
                    return true; // between 9 and A is "not restricted"
                }
                if (c < '[') {
                    continue; // means it's A - Z
                }
                if (c < 'a') { // before a
                    if (c == '_') { // only underscore is accepted
                        continue;
                    }
                    return true; // "not"
                }
                if (c > 'z') { // 122 is z, characters after of them are "not restricted"
                    return true;
                }
            }

            return false;
        }

        // limited-term = (A-Z, a-z, 0-9, dash 45, dot 46, fwd-slash 47, equals 61, underscore 95)+
        // kv-key-name = limited-term (dot limited-term)*
        public static bool NotNonWildcardKvKey(string s) {
            if (s[0] == '.') {
                return true; // can't start with dot
            }
            foreach (char c in s)
            {
                if (c < '0') { // before 0
                    if (c == '-' || c == '.' || c == '/') { // only dash dot and and fwd slash are accepted
                        continue;
                    }
                    return true; // "not"
                }
                if (c < ':') {
                    continue; // means it's 0 - 9
                }
                if (c < 'A') {
                    if (c == '=') { // equals is accepted
                        continue;
                    }
                    return true; // between 9 and A is "not limited"
                }
                if (c < '[') {
                    continue; // means it's A - Z
                }
                if (c < 'a') { // before a
                    if (c == '_') { // only underscore is accepted
                        continue;
                    }
                    return true; // "not"
                }
                if (c > 'z') { // 122 is z, characters after of them are "not limited"
                    return true;
                }
            }
            return false;
        }

        // (A-Z, a-z, 0-9, star 42, dash 45, dot 46, fwd-slash 47, equals 61, gt 62, underscore 95)+
        public static bool NotWildcardKvKey(string s) {
            if (s[0] == '.') {
                return true; // can't start with dot
            }
            foreach (char c in s)
            {
                if (c < '0') { // before 0
                    if (c == '*' || c == '-' || c == '.' || c == '/') { // only star dash dot and fwd slash are accepted
                        continue;
                    }
                    return true; // "not"
                }
                if (c < ':') {
                    continue; // means it's 0 - 9
                }
                if (c < 'A') {
                    if (c == '=' || c == '>') { // equals, gt is accepted
                        continue;
                    }
                    return true; // between 9 and A is "not limited"
                }
                if (c < '[') {
                    continue; // means it's A - Z
                }
                if (c < 'a') { // before a
                    if (c == '_') { // only underscore is accepted
                        continue;
                    }
                    return true; // "not"
                }
                if (c > 'z') { // 122 is z, characters after of them are "not limited"
                    return true;
                }
            }
            return false;
        }

        public static bool NotPrintableOrHasWildGt(string s) {
            return NotPrintableOrHasChars(s, WildGt);
        }

        public static bool NotPrintableOrHasWildGtDot(string s) {
            return NotPrintableOrHasChars(s, WildGtDot);
        }

        public static bool NotPrintableOrHasWildGtDollar(string s) {
            return NotPrintableOrHasChars(s, WildGtDollar);
        }

        public static string EmptyAsNull(string s)
        {
            return string.IsNullOrWhiteSpace(s) ? null : s;
        }

        public static bool ZeroOrLtMinus1(long l)
        {
            return l == 0 || l < -1;
        }
        
        public static Duration EnsureNotNullAndNotLessThanMin(Duration provided, Duration minimum, Duration dflt)
        {
            return provided == null || provided.Nanos < minimum.Nanos ? dflt : provided;
        }

        public static Duration EnsureDurationNotLessThanMin(long providedMillis, Duration minimum, Duration dflt)
        {
            return EnsureNotNullAndNotLessThanMin(Duration.OfMillis(providedMillis), minimum, dflt);
        }
         
        public static bool Equal(byte[] a, byte[] a2) {
            // exact same object or both null
            if (a == a2) { return true; }
            if (a == null || a2 == null) { return false; } // only one is null

            int length = a.Length;
            if (a2.Length != length) { return false; } // diff lengths

            for (int i=0; i<length; i++)
            {
                if (a[i] != a2[i]) { return false; } // diff byte
            }

            return true;
        }
 
        public static string EnsureEndsWithDot(string s) {
            return s == null || s.EndsWith(".") ? s : s + ".";
        }
    }
}