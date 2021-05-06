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
    public sealed class PullSubscribeOptions
    {
        public string Stream { get; }
        public ConsumerConfiguration ConsumerConfig { get; }
        public string Durable => ConsumerConfig.Durable;

        private PullSubscribeOptions(string stream, ConsumerConfiguration consumerConfig)
        {
            Stream = stream;
            ConsumerConfig = consumerConfig;
        }
        
        public sealed class Builder
        {
            private string _stream;
            private string _durable;
            private ConsumerConfiguration _consumerConfig;

            public Builder Stream(string stream)
            {
                _stream = stream;
                return this;
            }

            public Builder Durable(string durable)
            {
                _durable = durable;
                return this;
            }

            public Builder Configuration(ConsumerConfiguration consumerConfig)
            {
                _consumerConfig = consumerConfig;
                return this;
            }

            public PullSubscribeOptions Build() {
                Validator.ValidateStreamNameOrEmptyAsNull(_stream);

                _durable = Validator.ValidateDurableRequired(_durable, _consumerConfig);

                _consumerConfig = new ConsumerConfiguration.Builder(_consumerConfig)
                    .Durable(_durable)
                    .Build();

                return new PullSubscribeOptions(_stream, _consumerConfig);
            }
        }
    }
}
