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
    public class ChaosSimpleConsumer : ChaosConnectableConsumer
    {
        readonly IStreamContext sc;
        readonly IConsumerContext cc;
        readonly IOrderedConsumerContext occ;
        readonly IMessageConsumer mc;
        
        public ChaosSimpleConsumer(ChaosCommandLine cmd, ChaosConsumerKind consumerKind, int batchSize, int expiresIn) : base(cmd, "sc", consumerKind)
        {
            sc = Conn.GetStreamContext(cmd.Stream);

            ConsumeOptions co = ConsumeOptions.Builder()
                .WithBatchSize(batchSize)
                .WithExpiresIn(expiresIn)
                .Build();

            if (consumerKind == ChaosConsumerKind.Ordered) {
                OrderedConsumerConfiguration ocConfig = new OrderedConsumerConfiguration().WithFilterSubjects(cmd.Subject);
                cc = null;
                occ = sc.CreateOrderedConsumer(ocConfig);
                mc = occ.Consume(Handler, co);
            }
            else {
                occ = null;
                cc = sc.CreateOrUpdateConsumer(NewCreateConsumer().Build());
                mc = cc.Consume(Handler, co);
            }
            ChaosOutput.ControlMessage(Label, mc.GetConsumerName());
        }

        public override void refreshInfo()
        {
            UpdateLabel(mc.GetConsumerName());
        }
    }
}
