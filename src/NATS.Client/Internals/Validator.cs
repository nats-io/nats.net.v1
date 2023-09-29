// Copyright 2020-2023 The NATS Authors
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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace NATS.Client.Internals
{
    internal static class Validator
    {
        private static readonly char[] WildGt = { '*', '>'};
        private static readonly char[] WildGtDot = { '*', '>', '.'};
        private static readonly char[] WildGtDollar = {'*', '>', '$'};
        private static readonly char[] WildGtDotSlashes = { '*', '>', '.', '\\', '/'};
        
        internal static string Required(string s, string label) {
            if (EmptyAsNull(s) == null) {
                throw new ArgumentException($"{label} cannot be null or empty.");
            }
            return s;
        }
        
        internal static void Required(object o, string label) {
            if (o == null) {
                throw new ArgumentException($"{label} cannot be null or empty.");
            }
        }
        
        internal static void Required<TKey, TValue>(IDictionary<TKey, TValue> d, string label) {
            if (d == null || d.Count == 0) {
                throw new ArgumentException($"{label} cannot be null or empty.");
            }
        }
        
        /*
            cannot contain spaces \r \n \t
            cannot start or end with subject token delimiter .
            some things don't allow it to end greater
        */
        public static string ValidateSubjectTerm(string subject, string label, bool required)
        {
            Tuple<bool, string> t = IsValidSubjectTerm(subject, label, required);
            if (t.Item1)
            {
                return t.Item2;
            }
            throw new ArgumentException(t.Item2);
        }

        /*
         * If is valid, tuple item1 is true and item2 is the subject
         * If is not valid, tuple item1 is false and item2 is the error message
         */
        internal static Tuple<bool, string> IsValidSubjectTerm(string subject, string label, bool required) {
            subject = EmptyAsNull(subject);
            if (subject == null) {
                if (required) {
                    return new Tuple<bool, string>(false, $"{label} cannot be null or empty.");
                }
                return new Tuple<bool, string>(true, null);
            }
            if (subject.EndsWith(".")) {
                return new Tuple<bool, string>(false, $"{label} cannot end with '.'");
            }

            string[] segments = subject.Split('.');
            for (int seg = 0; seg < segments.Length; seg++) {
                string segment = segments[seg];
                int sl = segment.Length;
                if (sl == 0) {
                    if (seg == 0) {
                        return new Tuple<bool, string>(false, $"{label} cannot start with '.'");
                    }
                    return new Tuple<bool, string>(false, $"{label} segment cannot be empty");
                }
                else {
                    for (int m = 0; m < sl; m++) {
                        char c = segment[m];
                        switch (c) {
                            case ' ':
                            case '\r':
                            case '\n':
                            case '\t':
                                return new Tuple<bool, string>(false, $"{label} cannot contain space, tab, carriage return or linefeed character");
                            case '*':
                                if (sl != 1) {
                                    return new Tuple<bool, string>(false, $"{label} wildcard improperly placed.");
                                }
                                break;
                            case '>':
                                if (sl != 1 || seg != segments.Length - 1) {
                                    return new Tuple<bool, string>(false, $"{label} wildcard improperly placed.");
                                }
                                break;
                        }
                    }
                }
            }
            return new Tuple<bool, string>(true, subject);
        }

        public static string ValidateSubject(string s, bool required)
        {
            return ValidateSubject(s, "Subject", required, false);
        }
        
        public static string ValidateSubject(string subject, string label, bool required, bool cantEndWithGt) {
            subject = ValidateSubjectTerm(subject, label, required);
            if (subject != null && cantEndWithGt && subject.EndsWith(".>")) {
                throw new ArgumentException($"{label} last segment cannot be '>'");
            }
            return subject;
        }

        public static string ValidateReplyTo(string s, bool required) {
            return ValidatePrintableExceptWildGt(s, "Reply To", required);
        }

        public static string ValidateQueueName(string s, bool required) {
            return ValidatePrintableExceptWildDotGt(s, "Queue", required);
        }

        public static string ValidateStreamName(string s, bool required) {
            return ValidatePrintableExceptWildDotGtSlashes(s, "Stream", required);
        }

        public static string ValidateDurable(string s, bool required) {
            return ValidatePrintableExceptWildDotGtSlashes(s, "Durable", required);
        }

        public static string ValidateConsumerName(string s, bool required) {
            return ValidatePrintableExceptWildDotGtSlashes(s, "Name", required);
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

        public static string ValidatePrintableExceptWildDotGtSlashes(string s, string label, bool required)
        {
            return Validate(s, required, label, () => {
                if (NotPrintableOrHasWildGtDotSlashes(s)) {
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

        public static string ValidateIsRestrictedTerm(string s, string label, bool required)
        {
            return Validate(s, required, label, () => {
                if (NotRestrictedTerm(s)) {
                    throw new ArgumentException($"{label} must only contain A-Z, a-z, 0-9, `-` or `_` [{s}]");
                }
                return s;
            });
        }

        public static string ValidateBucketName(string s, bool required)
        {
            return ValidateIsRestrictedTerm(s, "Bucket Name", required);
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
            if (max < 1 || max > JetStreamConstants.MaxHistoryPerKey) {
                throw new ArgumentException($"Max History Per Key cannot be from 1 to {JetStreamConstants.MaxHistoryPerKey} inclusive.");
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

        internal static void ValidateDurationGtZeroRequired(long millis, string label) {
            if (millis <= 0) {
                throw new ArgumentException($"{label} duration must be supplied and greater than 0.");
            }
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

        internal static int ValidateGtZero(int i, string label)
        {
            if (i < 1)
            {
                throw new ArgumentException($"{label} must be greater than zero");
            }

            return i;
        }

        internal static int ValidateGtEqZero(int i, string label)
        {
            if (i < 0)
            {
                throw new ArgumentException($"{label} must be greater than or equal to zero");
            }

            return i;
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
        [Obsolete("This property is obsolete. use string.IsNullOrWhiteSpace(string) instead.", false)]
        public static bool NullOrEmpty(string s)
        {
            return string.IsNullOrWhiteSpace(s);
        }

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

        public static bool NotPrintableOrHasWildGtDotSlashes(string s) {
            return NotPrintableOrHasChars(s, WildGtDotSlashes);
        }
        
        public static bool NotPrintableOrHasWildGtDollar(string s) {
            return NotPrintableOrHasChars(s, WildGtDollar);
        }

        public static string EmptyAsNull(string s)
        {
            return string.IsNullOrWhiteSpace(s) ? null : s;
        }

        public static string EmptyOrNullAs(string s, string ifEmpty) {
            return string.IsNullOrWhiteSpace(s) ? ifEmpty : s;
        }

        public static IList<TSource> EmptyAsNull<TSource>(IList<TSource> list)
        {
            return EmptyOrNull(list) ? null : list;
        }

        public static bool EmptyOrNull<TSource>(IList<TSource> list)
        {
            return list == null || list.Count == 0;
        }

        public static bool EmptyOrNull<TSource>(TSource[] list)
        {
            return list == null || list.Length == 0;
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
        
        const string SemVerPattern = "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$";
        
        public static string ValidateSemVer(string s, string label, bool required)
        {
            return Validate(s, required, label, () => {
                if (!IsSemVer(s)) {
                    throw new ArgumentException($"{label} must be a valid SemVer [{s}]");
                }
                return s;
            });
        }

        public static bool IsSemVer(string s)
        {
            return Regex.IsMatch(s, SemVerPattern);
        }

        public static bool SequenceEqual<T>(IList<T> l1, IList<T> l2, bool nullSecondEqualsEmptyFirst = true)
        {
            if (l1 == null)
            {
                return l2 == null;
            }

            if (l2 == null)
            {
                return nullSecondEqualsEmptyFirst && l1.Count == 0;
            }
            
            return l1.SequenceEqual(l2);
        }

        // This function tests filter subject equivalency
        // It does not care what order and also assumes that there are no duplicates.
        // From the server: consumer subject filters cannot overlap [10138]
        public static bool ConsumerFilterSubjectsAreEquivalent<T>(IList<T> l1, IList<T> l2)
        {
            if (l1 == null || l1.Count == 0)
            {
                return l2 == null || l2.Count == 0;
            }

            if (l2 == null || l1.Count != l2.Count)
            {
                return false;
            }

            foreach (T t in l1) {
                if (!l2.Contains(t)) {
                    return false;
                }
            }
            return true;
        }

        public static bool DictionariesEqual(IDictionary<string, string> d1, IDictionary<string, string> d2)
        {
            if (d1 == d2)
            {
                return true;
            }
            
            if (d1 == null || d2 == null || d1.Count != d2.Count)
            {
                return false;
            }

            foreach (KeyValuePair<string, string> pair in d1)
            {
                if (!d2.TryGetValue(pair.Key, out var value) || !pair.Value.Equals(value))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
