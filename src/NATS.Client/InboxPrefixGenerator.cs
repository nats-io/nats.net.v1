using System;

namespace NATS.Client
{
    public delegate string InboxPrefixGenerator();

    /// <summary>
    /// Contains pre-made <see cref="InboxPrefixGenerator"/> generators or
    /// factories generating one.
    /// </summary>
    public static class InboxPrefixGenerators
    {
        /// <summary>
        /// Default <see cref="InboxPrefixGenerator"/> used by the client if none is specified.
        /// You can build upon it to create your own.
        /// </summary>
        public static InboxPrefixGenerator Default => () => IC.inboxPrefix;

        /// <summary>
        /// Builds upon <see cref="Default"/> and appends <paramref name="clientId"/> to the
        /// subject namespace.
        /// </summary>
        /// <param name="clientId"></param>
        /// <returns></returns>
        public static InboxPrefixGenerator WithClientId(Guid clientId) => () => $"{Default()}.{clientId:N}.";
    }
}