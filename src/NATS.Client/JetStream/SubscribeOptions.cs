// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// The base class for all Subscribe Options containing a stream and
    /// consumer configuration.
    /// </summary>
    public abstract class SubscribeOptions
    {
        internal string Stream { get; }
        internal bool Bind { get;  }
        internal ConsumerConfiguration ConsumerConfiguration { get;}

        internal SubscribeOptions(string stream, string durable, bool pull, bool bind, 
            string deliverSubject, string deliverGroup, ConsumerConfiguration cc)
        {
            Stream = Validator.ValidateStreamName(stream, bind);
            
            durable = Validator.ValidateMustMatchIfBothSupplied(durable, cc?.Durable, "Builder Durable", "Consumer Configuration Durable");
            durable = Validator.ValidateDurable(durable, pull || bind);

            deliverGroup = Validator.ValidateMustMatchIfBothSupplied(deliverGroup, cc?.DeliverGroup, "Builder Deliver Group", "Consumer Configuration Deliver Group");

            deliverSubject = Validator.ValidateMustMatchIfBothSupplied(deliverSubject, cc?.DeliverSubject, "Builder Deliver Subject", "Consumer Configuration Deliver Subject");

            ConsumerConfiguration = ConsumerConfiguration.Builder(cc)
                .WithDurable(durable)
                .WithDeliverSubject(deliverSubject)
                .WithDeliverGroup(deliverGroup)
                .Build();

            Bind = bind;
        }
    }
}
