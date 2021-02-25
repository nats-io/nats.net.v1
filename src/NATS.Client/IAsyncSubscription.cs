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
    /// <see cref="IAsyncSubscription"/> asynchronously delivers messages to listeners of the <see cref="MessageHandler"/>
    /// event.
    /// </summary>
    /// <remarks>
    /// If the <see cref="IAsyncSubscription"/> is created without listening to the <see cref="MessageHandler"/>
    /// event, no messages will be received until <see cref="Start()"/> has been called.
    /// </remarks>
    public interface IAsyncSubscription : ISubscription
    {
        /// <summary>
        /// Occurs when the <see cref="IAsyncSubscription"/> receives a message from the
        /// underlying <see cref="ISubscription"/>.
        /// </summary>
        event EventHandler<MsgHandlerEventArgs> MessageHandler;

        /// <summary>
        /// Starts delivering received messages to listeners on <see cref="MessageHandler"/>
        /// from a separate thread.
        /// </summary>
        /// <remarks>
        /// If the <see cref="IAsyncSubscription"/> has already started delivering messages, this
        /// method is a no-op.
        /// </remarks>
        void Start();
    }
}
