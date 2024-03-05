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

using System;
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    public class ChaosSimpleIterateConsumer : ChaosConnectableConsumer
    {
        readonly IStreamContext sc;
        readonly IConsumerContext cc;
        readonly Thread t;

        IIterableConsumer ic;
        
        public ChaosSimpleIterateConsumer(ChaosCommandLine cmd, ChaosConsumerKind consumerKind) : base(cmd, "it", consumerKind)
        {
            if (consumerKind == ChaosConsumerKind.Ordered) {
                throw new ArgumentException("Ordered Consumer not supported for Chaos Simple Iterate");
            }

            sc = Conn.GetStreamContext(cmd.Stream);

            cc = sc.CreateOrUpdateConsumer(NewCreateConsumer().Build());
            ChaosOutput.ControlMessage(Label, cc.ConsumerName);
            t = new Thread(() => run());
            t.Start();
        }

        public void run()
        {
            ChaosOutput.ControlMessage(Label, "Iterate");
            
            while (true)
            {
                try
                {
                    using (IIterableConsumer disposableIc = cc.Iterate())
                    {
                        ic = disposableIc;
                        Msg m = ic.NextMessage(1000);
                        if (m == null)
                        {
                            Thread.Sleep(10);
                        }
                        else
                        {
                            OnMessage(m);
                        }
                    }
                }
                catch (Exception e)
                {
                    // if there was an error, just try again
                    ChaosOutput.ControlMessage(Label, e.Message);
                }
            }
        }

        public override void refreshInfo()
        {
            if (ic != null) {
                UpdateLabel(cc.ConsumerName);
            }
        }
    }
}
