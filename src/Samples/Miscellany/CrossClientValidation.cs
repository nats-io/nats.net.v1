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
using System.Threading.Tasks;
using NATS.Client;
using NATS.Client.Service;

namespace NATSExamples
{
    // TO TEST, RUN THIS CLASS THEN THIS COMMAND:
    // deno run -A https://raw.githubusercontent.com/nats-io/nats.deno/main/tests/helpers/service-check.ts --server localhost:4222 --name JavaCrossClientValidator

    // TO RESET TEST CODE IF THERE ARE UPDATES:
    // deno cache --reload "https://raw.githubusercontent.com/nats-io/nats.deno/main/tests/helpers/service-check.ts"

    abstract class CrossClientValidation
    {
        public static void CrossClientValidationMain()
        {
            StatsDataSupplier sds = () => new CcvData(JsUtils.RandomText());
            StatsDataDecoder sdd = json =>
            {
                if (json.StartsWith("\"") && json.EndsWith("\"")) {
                    return new CcvData(json.Substring(1, json.Length - 1));
                }
                return new CcvData(json);            
            };

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = "nats://localhost:4222";

            using (IConnection nc = new ConnectionFactory().CreateConnection(opts))
            {
                EventHandler<MsgHandlerEventArgs> handler = (sender, args) =>
                {
                    byte[] payload = args.Message.Data;
                    if (payload == null || payload.Length == 0)
                    {
                        ServiceMessage.ReplyStandardError(nc, args.Message, "need a string", 400);
                    }
                    else
                    {
                        string data = Encoding.UTF8.GetString(payload);
                        if (data.Equals("error"))
                        {
                            throw new Exception("service asked to throw an error");
                        }
                        else
                        {
                            ServiceMessage.Reply(nc, args.Message, payload);
                        }
                    }
                };
                
                // create the services
                Service service = new ServiceBuilder()
                    .WithConnection(nc)
                    .WithName("JavaCrossClientValidator")
                    .WithSubject("jccv")
                    .WithDescription("Java Cross Client Validator")
                    .WithVersion("0.0.1")
                    .WithSchemaRequest("schema request string/url")
                    .WithSchemaResponse("schema response string/url")
                    .WithStatsDataHandlers(sds, sdd)
                    .WithServiceMessageHandler(handler)
                    .Build();

                Console.WriteLine(service);

                Task<bool> task = service.StartService();

                Msg msg = nc.Request("jccv", Encoding.UTF8.GetBytes("hello"));
                string response = Encoding.UTF8.GetString(msg.Data);
                Console.WriteLine("Called jccv with 'hello'. Received [" + response + "]");

                msg = nc.Request("jccv", null);
                string se = msg.Header[ServiceMessage.NatsServiceError];
                string sec = msg.Header[ServiceMessage.NatsServiceErrorCode];
                Console.WriteLine("Called jccv with null. Received [" + se + ", " + sec + "]");

                msg = nc.Request("jccv", Encoding.UTF8.GetBytes(""));
                se = msg.Header[ServiceMessage.NatsServiceError];
                sec = msg.Header[ServiceMessage.NatsServiceErrorCode];
                Console.WriteLine("Called jccv with empty. Received [" + se + ", " + sec + "]");

                msg = nc.Request("jccv", Encoding.UTF8.GetBytes("error"));
                se = msg.Header[ServiceMessage.NatsServiceError];
                sec = msg.Header[ServiceMessage.NatsServiceErrorCode];
                Console.WriteLine("Called jccv with 'error'. Received [" + se + ", " + sec + "]");
                
                try
                {
                    task.Wait(60000);
                }
                catch (Exception) {
                    // We expect this to timeout because we don't stop the service.
                    // You can just stop the program also.
                }
            }
        }
    }
    
    class CcvData : IStatsData {
        string Text;

        public CcvData(string text) {
            Text = text;
        }

        public string ToJson()
        {
            return "\"" + Text + "\"";
        }
    }
}