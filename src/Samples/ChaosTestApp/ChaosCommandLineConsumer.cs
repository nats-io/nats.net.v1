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

namespace NATSExamples
{
    public class ChaosCommandLineConsumer
    {
        public readonly ChaosConsumerType consumerType;
        public readonly ChaosConsumerKind consumerKind;
        public readonly int batchSize;
        public readonly int expiresIn;

        public ChaosCommandLineConsumer(String consumerKind) {
            this.consumerType = ChaosConsumerType.Push;
            this.consumerKind = ChaosEnums.ConsumerKindInstance(consumerKind);
            batchSize = 0;
            expiresIn = 0;
        }

        public ChaosCommandLineConsumer(String consumerType, String consumerKind, int batchSize, int expiresIn) {
            this.consumerType = ChaosEnums.ConsumerTypeInstance(consumerType);
            this.consumerKind = ChaosEnums.ConsumerKindInstance(consumerKind);
            if (batchSize < 1) {
                throw new ArgumentException("Invalid Batch Size:" + batchSize);
            }
            this.batchSize = batchSize;
            if (expiresIn < 1_000) {
                throw new ArgumentException("Expires must be >= 1000ms");
            }
            this.expiresIn = expiresIn;
        }

        public override string ToString()
        {
            if (consumerType == ChaosConsumerType.Simple) {
                return consumerType.ToString().ToLower() +
                       " " + consumerKind.ToString().ToLower() +
                       " " + batchSize +
                       " " + expiresIn;
            }
            return consumerType.ToString().ToLower() +
                   " " + consumerKind.ToString().ToLower();
        }
    }
}
