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

namespace NATS.Client.JetStream
{
    /// <summary>
    /// The base class for all Subscribe Options containing a stream and
    /// consumer configuration.
    /// </summary>
    public class SubscribeOptions
    {
        internal string Stream { get; }
        internal bool Direct { get;  }
        internal ConsumerConfiguration ConsumerConfiguration { get;}

        internal SubscribeOptions(string stream, bool direct, ConsumerConfiguration configuration)
        {
            // THESE ARE ALREADY BE VALIDATED BY PRIVATE CONSTRUCTORS THAT CALL THIS base METHOD!
            Stream = stream;
            Direct = direct;
            ConsumerConfiguration = configuration;
        }
    }
}
