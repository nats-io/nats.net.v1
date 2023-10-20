// Copyright 2023 The NATS Authors
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
using System.Threading;
using System.Threading.Tasks;
using NATS.Client;

namespace NATSExamples.ClientCompatibility
{
    public static class ClientCompatibility
    {
        public static void ClientCompatibilityMain(string url) {
            if (url == null) 
            {
                url = Environment.GetEnvironmentVariable("NATS_URL");
                if (url == null) {
                    url = "nats://localhost:4222";
                }
            }
            Log.info("Url: " + url);

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = url;

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    c.SubscribeAsync("tests.>", (obj, ea) =>
                    {
                        try
                        {
                            TestMessage testMessage = new TestMessage(ea.Message);
                            if (testMessage.suite == Suite.Done)
                            {
                                Environment.Exit(0);
                            }

                            if (testMessage.kind == Kind.Result)
                            {
                                string p = testMessage.payload == null
                                    ? ""
                                    : Encoding.UTF8.GetString(testMessage.payload);
                                if (testMessage.something.Equals("pass"))
                                {
                                    Log.info("PASS", testMessage.subject, p);
                                }
                                else
                                {
                                    Log.error("FAIL", testMessage.subject, p);
                                }

                                return;
                            }

                            Task.Run(() =>
                            {
                                try
                                {
                                    //noinspection SwitchStatementWithTooFewBranches
                                    switch (testMessage.suite)
                                    {
                                        case Suite.ObjectStore:
                                            new ObjectStoreCommand(c, testMessage).execute();
                                            break;
                                        default:
                                            Log.error("UNSUPPORTED SUITE: " + testMessage.suite);
                                            break;
                                    }
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine(e);
                                    Environment.Exit(-2);
                                }
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.ToString());
                            Environment.Exit(-1);
                        }
                    });

                    Log.info("Ready");
                    Thread.Sleep(600000);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
    }
}
