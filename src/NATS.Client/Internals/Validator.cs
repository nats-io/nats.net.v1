using System;
using NATS.Client.Jetstream.Api;

namespace NATS.Client.Internals
{
    internal static class Validator
    {
        internal static string ValidateMessageSubjectRequired(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                throw new ArgumentException("Subject cannot be null or empty [" + s + "]");
            }

            return s;
        }

        internal static string ValidateJsSubscribeSubjectRequired(string s)
        {
            if (string.IsNullOrEmpty(s) || ContainsWhitespace(s))
            {
                throw new ArgumentException("Subject cannot have whitespace if provided [" + s + "]");
            }

            return s;
        }

        internal static string ValidateQueueNameRequired(string s)
        {
            if (string.IsNullOrEmpty(s) || ContainsWhitespace(s))
            {
                throw new ArgumentException("Queue have whitespace [" + s + "]");
            }

            return EmptyAsNull(s);
        }

        internal static string ValidateReplyToNullButNotEmpty(string s)
        {
            if (NotNullButEmpty(s))
            {
                throw new ArgumentException("ReplyTo cannot be blank when provided [" + s + "]");
            }

            return s;
        }

        internal static string ValidateStreamName(string s)
        {
            if (ContainsDotWildGt(s))
            {
                throw new ArgumentException("Stream cannot contain a '.', '*' or '>' [" + s + "]");
            }

            return s;
        }

        internal static string ValidateStreamNameOrEmptyAsNull(string s)
        {
            return EmptyAsNull(ValidateStreamName(s));
        }

        internal static string ValidateStreamNameRequired(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                throw new ArgumentException(
                    "Stream cannot be null or empty and cannot contain a '.', '*' or '>' [" + s + "]");
            }

            return ValidateStreamName(s);
        }

        internal static string ValidateDurableOrEmptyAsNull(string s)
        {
            if (ContainsDotWildGt(s))
            {
                throw new ArgumentException("Durable cannot contain a '.', '*' or '>' [" + s + "]");
            }

            return EmptyAsNull(s);
        }

        internal static string ValidateDurableRequired(string durable, ConsumerConfiguration cc)
        {
            if (durable == null)
            {
                if (cc == null)
                {
                    throw new ArgumentException(
                        "Durable is required and cannot contain a '.', '*' or '>' [null]");
                }

                return ValidateDurableRequired(cc.Durable);
            }

            return ValidateDurableRequired(durable);
        }

        internal static string ValidateDurableRequired(string s)
        {
            if (string.IsNullOrEmpty(s) || ContainsDotWildGt(s))
            {
                throw new ArgumentException("Durable is required and cannot contain a '.', '*' or '>' [" + s +
                                                   "]");
            }

            return s;
        }

        internal static int ValidatePullBatchSize(int pullBatchSize)
        {
            if (pullBatchSize < 1 || pullBatchSize > NatsJetStreamConstants.MaxPullSize)
            {
                throw new ArgumentException("Pull Batch Size must be between 1 and " + NatsJetStreamConstants.MaxPullSize +
                                                   " inclusive [" + pullBatchSize + "]");
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
                return Duration.ZERO;
            }

            if (d.IsNegative())
            {
                throw new ArgumentException("Duration must be greater than or equal to 0.");
            }

            return d;
        }

        internal static string ValidateJetStreamPrefix(string s)
        {
            if (ContainsWildGtDollarSpaceTab(s))
            {
                throw new ArgumentException("Prefix cannot contain a wildcard or dollar sign [" + s + "]");
            }

            return s;
        }

        internal static object ValidateNotNull(object o, string fieldName)
        {
            if (o == null)
            {
                throw new ArgumentException(fieldName + " cannot be null");
            }

            return o;
        }

        internal static string ValidateNotNull(string s, string fieldName)
        {
            if (s == null)
            {
                throw new ArgumentException(fieldName + " cannot be null");
            }

            return s;
        }

        internal static long ValidateGtZeroOrMinus1(long l, string label)
        {
            if (ZeroOrLtMinus1(l))
            {
                throw new ArgumentException(label + " must be greater than zero or -1 for unlimited");
            }

            return l;
        }

        // ----------------------------------------------------------------------------------------------------
        // Helpers
        // ----------------------------------------------------------------------------------------------------
        internal static bool NotNullButEmpty(string s)
        {
            return s != null && s.Length == 0;
        }

        internal static bool ContainsWhitespace(string s)
        {
            if (s != null)
            {
                for (int i = 0; i < s.Length; i++)
                {
                    if (char.IsWhiteSpace(s[i]))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        internal static bool ContainsDotWildGt(string s)
        {
            if (s != null)
            {
                for (int i = 0; i < s.Length; i++)
                {
                    switch (s[i])
                    {
                        case '.':
                        case '*':
                        case '>':
                            return true;
                    }
                }
            }

            return false;
        }

        internal static bool ContainsWildGtDollarSpaceTab(string s)
        {
            if (s != null)
            {
                for (int i = 0; i < s.Length; i++)
                {
                    switch (s[i])
                    {
                        case '*':
                        case '>':
                        case '$':
                        case ' ':
                        case '\t':
                            return true;
                    }
                }
            }

            return false;
        }

        internal static string EmptyAsNull(string s)
        {
            return string.IsNullOrEmpty(s) ? null : s;
        }

        internal static bool ZeroOrLtMinus1(long l)
        {
            return l == 0 || l < -1;
        }
    }
}