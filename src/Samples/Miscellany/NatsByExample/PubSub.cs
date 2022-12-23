// Copyright 2022 The NATS Authors
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
using System.Text;
using NATS.Client;

namespace NATSExamples.NatsByExample
{
    abstract class PubSub
    {
        public static void PubSubMain()
        {
            string natsUrl = Environment.GetEnvironmentVariable("NATS_URL");
            if (natsUrl == null)
            {
                natsUrl = "nats://127.0.0.1:4222";
            }

            // Create a new connection factory to create a connection.
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = natsUrl;

            // Creates a connection to nats server at the `natsUrl`
            // An `IConnection` is `IDisposable` so it can be use
            // within `using` statement. 
            ConnectionFactory cf = new ConnectionFactory();
            IConnection c = cf.CreateConnection(opts);

            // Here are some of the accessible properties from
            // the Msg object:
            //   - Msg.Data;
            //   - Msg.Reply;
            //   - Msg.Subject;
            //   - Msg.Header;
            //   - Msg.MetaData;

            // Setup an event handler to process incoming messages.
            // An anonymous delegate function is used for brevity.
            EventHandler<MsgHandlerEventArgs> handler = (sender, args) =>
            {
                Msg m = args.Message;
                string text = Encoding.UTF8.GetString(m.Data);
                Console.WriteLine($"Async handler received the message '{text}' from subject '{m.Subject}'");
            };

            // Subscriptions will only receive messages that are
            // published after the subscription is made, so for this
            // example, we will publish a message before we are subscribed
            // which will not be received.
            c.Publish("greet.joe", Encoding.UTF8.GetBytes("hello joe 1"));
            
            // The simple way to create an asynchronous subscriber
            // is to simply pass the handler in. Messages will start
            // arriving immediately. We are subscribing to anything that
            // matches the `greet.*` pattern. You will see that this
            // subscription will not receive the "hello joe 1" message
            IAsyncSubscription subAsync = c.SubscribeAsync("greet.*", handler);

            // Simple synchronous subscriber. In this case we are only
            // subscribing to greetings to pam.
            ISyncSubscription subSync = c.SubscribeSync("greet.pam");

            // Lets publish to two different greeting messages
            c.Publish("greet.pam", Encoding.UTF8.GetBytes("hello pam 1"));
            c.Publish("greet.joe", Encoding.UTF8.GetBytes("hello joe 2"));

            // Using a synchronous subscriber, try to get some messages,
            // waiting up to 1000 milliseconds (1 second).
            try
            {
                Msg m = subSync.NextMessage(1000);
                string text = Encoding.UTF8.GetString(m.Data);
                Console.WriteLine($"Sync subscription received the message '{text}' from subject '{m.Subject}'");
                m = subSync.NextMessage(100);
            }
            // Catching the NATSTimeoutException will let you know there were no
            // messages available. In this case, there was only one message,
            // so the second NextMessage timed out.
            catch (NATSTimeoutException)
            {
                Console.WriteLine($"Sync subscription no messages currently available");
            }

            // Calling drain directly on the connection also closes the
            // connection. If you want to keep the connection open and do
            // other things, you could call drain directly on the subscription,
            // in this example either `subAsync.Drain();` or `subSync.Drain();`
            c.Drain();
        }
    }
}