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
    /// Interface definition for a Fetch Consumer
    /// </summary>
    public interface IFetchConsumer : IMessageConsumer
    {
        /// <summary>
        /// Read the next message. Return null if the fetch has been fulfilled either
        /// because max messages or bytes max bytes have been reached,
        /// or because the fetch was not fulfilled in the timeout set byt the fetch options.
        /// @return the next message for this subscriber or null if there is a timeout
        /// </summary>
        /// <returns>the next message or null if there is a timeout</returns>
        Msg NextMessage();
    }
}
