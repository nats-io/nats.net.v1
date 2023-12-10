using System;

namespace NATSExamples
{
    public class CommandLineConsumer
    {
        public readonly ConsumerType consumerType;
        public readonly ConsumerKind consumerKind;
        public readonly int batchSize;
        public readonly int expiresIn;

        public CommandLineConsumer(String consumerKind) {
            this.consumerType = ConsumerType.Push;
            this.consumerKind = Enums.ConsumerKindInstance(consumerKind);
            batchSize = 0;
            expiresIn = 0;
        }

        public CommandLineConsumer(String consumerType, String consumerKind, int batchSize, int expiresIn) {
            this.consumerType = Enums.ConsumerTypeInstance(consumerType);
            this.consumerKind = Enums.ConsumerKindInstance(consumerKind);
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
            if (consumerType == ConsumerType.Simple) {
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