// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
    public interface IAsyncSubscription : ISubscription, IDisposable
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
