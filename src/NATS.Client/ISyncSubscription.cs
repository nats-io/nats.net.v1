// Copyright 2015-2018 The NATS Authors
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
