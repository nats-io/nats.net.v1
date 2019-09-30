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
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NATS.Client
{
    /// <summary>
    /// Tracks various statistics received and sent on an <see cref="IConnection"/>.
    /// </summary>
    public interface IStatistics
    {
        /// <summary>
        /// Gets the number of inbound messages received.
        /// </summary>
        long InMsgs { get; }

        /// <summary>
        /// Gets the number of messages sent.
        /// </summary>
        long OutMsgs { get; }

        /// <summary>
        /// Gets the number of incoming bytes.
        /// </summary>
        long InBytes { get; }

        /// <summary>
        /// Gets the outgoing number of bytes.
        /// </summary>
        long OutBytes { get; }

        /// <summary>
        /// Gets the number of reconnections.
        /// </summary>
        long Reconnects { get; }
    }
}
