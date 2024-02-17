// Copyright 2024 The NATS Authors
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

using NATS.Client.JetStream;

namespace NATSExamples
{
    public class ChaosPushConsumer : ChaosConnectableConsumer
    {
        readonly IJetStreamPushAsyncSubscription sub;
        
        public ChaosPushConsumer(ChaosCommandLine cmd, ChaosConsumerKind consumerKind) : base(cmd, "pu", consumerKind)
        {
            PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                .WithStream(cmd.Stream)
                .WithConfiguration(NewCreateConsumer()
                    .WithIdleHeartbeat(1000)
                    .Build())
                .WithOrdered(consumerKind == ChaosConsumerKind.Ordered)
                .Build();

            sub = Js.PushSubscribeAsync(cmd.Subject, Handler, false, pso);
        }

        public override void refreshInfo()
        {
            UpdateLabel(sub.Consumer);
        }
    }
}
