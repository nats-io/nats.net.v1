// Copyright 2022 The NATS Authors
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

using JsMulti.Settings;
using NATS.Client;
using NATS.Client.JetStream;

namespace JsMulti.Shared
{
    public interface IOptionsFactory
    {
        Options GetOptions(Context ctx);
        JetStreamOptions GetJetStreamOptions(Context ctx);
    }
    
    public class OptionsFactory : IOptionsFactory
    {
        public Options GetOptions(Context ctx)
        {
            return GetOptions(ctx.Server);
        }

        public JetStreamOptions GetJetStreamOptions(Context ctx)
        {
            return JetStreamOptions.DefaultJsOptions;
        }
         
        public static Options GetOptions(string server)
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = server;
            opts.Timeout = 5000;
            opts.PingInterval = 10000;
            opts.ReconnectWait = 1000;
            opts.ClosedEventHandler =
                opts.DisconnectedEventHandler = 
                    (sender, args) => {}; // both are EventHandler<ConnEventArgs> 
            opts.FlowControlProcessedEventHandler = (sender, args) => {}; // EventHandler<FlowControlProcessedEventArgs>
            return opts;
        }
        
        public static JetStreamOptions GetJetStreamOptions()
        {
            return JetStreamOptions.DefaultJsOptions;
        }
    }
}