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
    public class ChaosSimpleFetchConsumer : ChaosConnectableConsumer
    {
        readonly IStreamContext sc;
        readonly IConsumerContext cc;
        readonly int batchSize;
        readonly int expiresIn;
        readonly Thread t;

        IFetchConsumer fc;
        
        public ChaosSimpleFetchConsumer(ChaosCommandLine cmd, ChaosConsumerKind consumerKind, int batchSize, int expiresIn) : base(cmd, "fc", consumerKind)
        {
            if (consumerKind == ChaosConsumerKind.Ordered) {
                throw new ArgumentException("Ordered Consumer not supported for App Simple Fetch");
            }

            this.batchSize = batchSize;
            this.expiresIn = expiresIn;

            sc = Conn.GetStreamContext(cmd.Stream);

            cc = sc.CreateOrUpdateConsumer(NewCreateConsumer().Build());
            ChaosOutput.ControlMessage(Label, cc.ConsumerName);
            t = new Thread(() => run());
            t.Start();
        }

        public void run()
        {
            FetchConsumeOptions fco = FetchConsumeOptions.Builder().WithMaxMessages(batchSize).WithExpiresIn(expiresIn).Build();
            ChaosOutput.ControlMessage(Label, ToString(fco));
            
            while (true)
            {
                try
                {
                    using (IFetchConsumer disposableFc = cc.Fetch(fco))
                    {
                        fc = disposableFc;
                        Msg m = fc.NextMessage();
                        while (m != null)
                        {
                            OnMessage(m);
                            m = fc.NextMessage();
                        }
                    }
                }
                catch (Exception)
                {
                    // if there was an error, just try again
                }

                // simulating some work to be done between fetches
                ChaosOutput.WorkMessage(Label, "Fetch Batch Completed, Last Received Seq: " + lastReceivedSequence.Read());
                Thread.Sleep(10);
            }
        }

        public override void refreshInfo()
        {
            if (fc != null) {
                UpdateLabel(cc.ConsumerName);
            }
        }
        
        
        public static String ToString(FetchConsumeOptions fco) {
            return "FetchConsumeOptions" +
                   "\nMax Messages: " + fco.MaxMessages +
                   "\nMax Bytes: " + fco.MaxBytes +
                   "\nExpires In: " + fco.ExpiresInMillis +
                   "\nIdleHeartbeat: " + fco.IdleHeartbeat +
                   "\nThreshold Percent: " + fco.ThresholdPercent;
        }

    }
}
