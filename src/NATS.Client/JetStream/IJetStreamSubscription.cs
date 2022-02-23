// Copyright 2021 The NATS Authors
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

using System.Collections.Generic;

namespace NATS.Client.JetStream
{
    public interface IJetStreamSubscription : ISubscription
    {
        /// <summary>
        /// The JetStream Context
        /// </summary>
        JetStream Context { get; }

        /// <summary>
        /// The Stream Name
        /// </summary>
        string Stream { get; }

        /// <summary>
        /// The Consumer Name
        /// </summary>
        string Consumer { get; }

        /// <summary>
        /// The Deliver Subject
        /// </summary>
        string DeliverSubject { get; }

        /// <summary>
        /// Gets the ConsumerInformation for this Subscription.
        /// </summary>
        ConsumerInfo GetConsumerInformation();

        bool IsPullMode();
    }

    public interface IJetStreamPushSyncSubscription : IJetStreamSubscription, ISyncSubscription
    {
    }

    public interface IJetStreamPushAsyncSubscription : IJetStreamSubscription, IAsyncSubscription
    {
    }
    
    /// <summary>
    /// Pull Subscription on a JetStream context.
    /// </summary>
    public interface IJetStreamPullSubscription : IJetStreamSubscription, ISyncSubscription
    {
        /// <summary>
        /// Polls for new messages, overriding the default batch size for this pull only.
        /// </summary>
        /// <remarks>
        ///
        /// Primitive API for Advanced use only. Prefer Fetch 
        /// </remarks>
        /// <param name="batchSize">the size of the batch</param>
        void Pull(int batchSize);

        /// <summary>
        /// Do a pull in noWait mode with the specified batch size.
        /// </summary>
        /// <remarks>
        ///
        /// Primitive API for Advanced use only. Prefer Fetch 
        /// </remarks>
        /// <param name="batchSize">the size of the batch</param>
        void PullNoWait(int batchSize);

        /// <summary>
        /// Do a pull in noWait + expire mode with the specified batch size.
        /// </summary>
        /// <remarks>
        ///
        /// Primitive API for Advanced use only. Prefer Fetch 
        /// </remarks>
        /// <param name="batchSize">the size of the batch</param>
        /// <param name="expiresInMillis">how long from now the server should expire this request</param>
        void PullNoWait(int batchSize, int expiresInMillis);

        /// <summary>
        /// Initiate pull for all messages available before expiration.
        /// </summary>
        /// <remarks>
        ///
        /// Primitive API for Advanced use only. Prefer Fetch 
        /// </remarks>
        /// <param name="batchSize">the size of the batch</param>
        /// <param name="expiresInMillis">how long from now the server should expire this request</param>
        void PullExpiresIn(int batchSize, int expiresInMillis);

        /// <summary>
        /// Fetch a list of messages up to the batch size, waiting no longer than maxWait.
        /// </summary>
        /// <remarks>
        /// This uses <code>pullExpiresIn</code> under the covers, and manages all responses
        /// from<code> sub.NextMessage(...)</code> to only return regular JetStream messages.
        /// </remarks>
        /// <param name="batchSize">the size of the batch</param>
        /// <param name="maxWaitMillis">The maximum time to wait for the first message.</param>
        /// <returns></returns>
        IList<Msg> Fetch(int batchSize, int maxWaitMillis);
    }
}