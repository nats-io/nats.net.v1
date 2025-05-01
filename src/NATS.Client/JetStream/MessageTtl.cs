using System;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Class to make setting a per message ttl easier.
    /// </summary>
    public sealed class MessageTtl
    {          
        public string TtlString { get; }

        internal MessageTtl(string ttlString)
        {
            TtlString = ttlString;
        }
 
        /// <summary>
        /// Sets the TTL for this specific message to be published 
        /// </summary>
        /// <param name="msgTtlSeconds">the ttl in seconds</param>
        /// <returns>The MessageTtl instance</returns>
        /// <exception cref="ArgumentException">Must be at least 1 second</exception>
        public static MessageTtl Seconds(int msgTtlSeconds) {
            if (msgTtlSeconds < 1) {
                throw new ArgumentException("Must be at least 1 second.");
            }
            return new MessageTtl(msgTtlSeconds + "s");
        }

        /// <summary>
        /// Sets the TTL for this specific message to be published. Use at your own risk.
        /// The current specification can be found here See <a href="https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-43.md#per-message-ttl">JetStream Per-Message TTL</a>
        /// </summary>
        /// <param name="msgTtlCustom"> the custom ttl string</param>
        /// <returns>The MessageTtl instance</returns>
        /// <exception cref="ArgumentException">A value is required</exception>
        public static MessageTtl Custom(String msgTtlCustom) {
            if (string.IsNullOrWhiteSpace(msgTtlCustom)) {
                throw new ArgumentException("Custom value required.");
            }
            return new MessageTtl(msgTtlCustom);
        }

        /// <summary>
        /// Sets the TTL for this specific message to be published and never be expired
        /// </summary>
        /// <returns>The MessageTtl instance</returns>
        public static MessageTtl Never() {
            return new MessageTtl("never");
        }
    }
}