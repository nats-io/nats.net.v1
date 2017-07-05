// Copyright 2015 Apcera Inc. All rights reserved.

using System;

namespace NATS.Client
{
    /// <summary>
    /// <see cref="ISyncSubscription"/> provides messages for a subject through calls
    /// to <see cref="NextMessage()"/> and <see cref="NextMessage(int)"/>.
    /// </summary>
    public interface ISyncSubscription : ISubscription, IDisposable
    {
        /// <summary>
        /// Returns the next <see cref="Msg"/> available to a synchronous
        /// subscriber, blocking until one is available.
        /// </summary>
        /// <returns>The next <see cref="Msg"/> available to a subscriber.</returns>
        Msg NextMessage();

        /// <summary>
        /// Returns the next <see cref="Msg"/> available to a synchronous
        /// subscriber, or block up to a given timeout until the next one is available.
        /// </summary>
        /// <param name="timeout">The amount of time, in milliseconds, to wait for
        /// the next message.</param>
        /// <returns>The next <see cref="Msg"/> available to a subscriber.</returns>
        Msg NextMessage(int timeout);
    }
}
