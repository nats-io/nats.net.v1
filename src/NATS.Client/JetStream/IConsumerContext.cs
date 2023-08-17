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

namespace NATS.Client.JetStream
{
    /// <summary>
    /// The Consumer Context provides a convenient interface around a defined JetStream Consumer
    /// </summary>
    public interface IConsumerContext : IBaseConsumerContext
    {
        /// <summary>
        /// Gets the consumer name that was used to create the context.
        /// </summary>
        /// <returns>the consumer name</returns>
        string ConsumerName { get; }

        /// <summary>
        /// Gets information about the consumer behind this subscription.
        /// </summary>
        /// <returns>consumer information</returns>
        ConsumerInfo GetConsumerInfo();

        /// <summary>
        /// Gets information about the consumer behind this subscription.
        /// This returns the last read version of Consumer Info, which could technically be out of date.
        /// </summary>
        /// <returns>consumer information</returns>
        ConsumerInfo GetCachedConsumerInfo();
    }
}
