﻿// Copyright 2023 The NATS Authors
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
    /// Interface definition for a Iterable Consumer
    /// </summary>
    public interface IIterableConsumer : IMessageConsumer
    {
        /// <summary>
        /// Read the next message. Return null if the calls times out.
        /// Use a timeout of 0 to wait indefinitely. This could still be interrupted if
        /// the subscription is unsubscribed or the client connection is closed.
        /// </summary>
        /// <param name="timeoutMillis">the maximum time to wait</param>
        /// <returns>the next message for this subscriber.</returns>
        Msg NextMessage(int timeoutMillis);
    }
}
