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
    public sealed class JetStreamOptions
    {
        internal static readonly Duration DefaultTimeout = Duration.OfMillis(Defaults.Timeout);

        public string Prefix { get; }
        public Duration RequestTimeout { get; }
        public bool PublishNoAck { get; }

        public JetStreamOptions(string prefix, Duration requestTimeout, bool publishNoAck)
        {
            Prefix = prefix;
            RequestTimeout = requestTimeout;
            PublishNoAck = publishNoAck;
        }

        public sealed class Builder
        {
            private string _prefix;
            private Duration _requestTimeout;
            private bool _publishNoAck;
            
            public Builder JetStreamOptions(JetStreamOptions jso) {
                if (jso == null) return this;
                _prefix = jso.Prefix;
                _requestTimeout = jso.RequestTimeout;
                _publishNoAck = jso.PublishNoAck;
                return this;
            }
            
            public Builder Prefix(string prefix) {
                _prefix = prefix;
                return this;
            }

            public Builder RequestTimeout(Duration requestTimeout) {
                _requestTimeout = requestTimeout;
                return this;
            }

            public Builder PublishNoAck(bool publishNoAck)
            {
                _publishNoAck = publishNoAck;
                return this;
            }

            public JetStreamOptions Build()
            {
                _prefix = JsPrefixManager.AddPrefix(_prefix);
                _requestTimeout = _requestTimeout ?? DefaultTimeout;
                return new JetStreamOptions(_prefix, _requestTimeout, _publishNoAck);
            }
        }
    }
}
