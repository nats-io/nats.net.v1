// Copyright 2023 The NATS Authors
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

namespace NATS.Client.JetStream
{
    /// <summary>
    /// SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
    /// </summary>
    public interface IMessageConsumer : IDisposable
    {
        /// <summary>
        /// Gets information about the consumer behind this subscription.
        /// </summary>
        /// <returns>consumer information</returns>
        ConsumerInfo GetConsumerInformation();

        /// <summary>
        /// Gets information about the consumer behind this subscription.
        /// This returns the last read version of Consumer Info, which could technically be out of date.
        /// </summary>
        /// <returns>consumer information</returns>
        ConsumerInfo GetCachedConsumerInformation();

        /// <summary>
        /// Stop the Simple Consumer from asking for any more messages from the server.
        /// Messages do not immediately stop
        /// </summary>
        /// <param name="timeout">The time to wait for the stop to succeed, pass 0 to wait forever.
        /// Stop involves moving messages to and from the server so a very short timeout is not recommended.</param>
        /// <returns>A task so you could wait for the stop to know when there are no more messages.</returns>
        void Stop(int timeout);
    }
}
