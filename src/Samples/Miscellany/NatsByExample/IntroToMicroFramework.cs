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
using System.Threading.Tasks;
using NATS.Client;
using NATS.Client.Service;

namespace NATSExamples.NatsByExample
{
    abstract class IntroToMicroFramework
    {
        public static void IntroToMicroFrameworkMain()
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
            using (IConnection c = cf.CreateConnection(opts))
            {
                // ### What is a Service?
                //
                // A "service" consists of one or more endpoints.
                // An endpoint can be part of a group of endpoints or by itself.
    
                // ### Defining a Group
                //
                // In this example, the services will be part of a group.
                // The group name will be the prefix for the subject of the request.
                // Alternatively you could manually specify the group's subject
                // Here we create the group.
                Group serviceGroup = new Group("minmax");
    
                // ### Defining Endpoints
                // For each endpoint we give it a name. Like group,
                // you could manually specify the endpoint's subject.
                // In this example we are adding the endpoint to the group we defined
                // and are providing a ServiceEventHandler<MsgHandlerEventArgs> implementation
                ServiceEndpoint min = ServiceEndpoint.Builder()
                    .WithEndpointName("min")
                    .WithGroup(serviceGroup)
                    .WithHandler((s, a) => minRequestHandler(c, a.Message))
                    .Build();
    
                ServiceEndpoint max = ServiceEndpoint.Builder()
                    .WithEndpointName("max")
                    .WithGroup(serviceGroup)
                    .WithHandler((s, a) => maxRequestHandler(c, a.Message))
                    .Build();
    
                // ### Defining the Service
                //
                // The Service definition requires a name and version, description is optional.
                // The name must be a simple name consisting of the characters A-Z, a-z, 0-9, dash (-) or underscore (_).
                // Add the endpoints that were created. Give the service a connection to run on.
                // A unique id is created for the service to identify it from different instances of the service.
                Service service = Service.Builder()
                    .WithName("minmax")
                    .WithVersion("0.0.1")
                    .WithDescription("Returns the min/max number in a request")
                    .AddServiceEndpoint(min)
                    .AddServiceEndpoint(max)
                    .WithConnection(c)
                    .Build();
    
                Console.WriteLine("Created Service: " + service.Name + " with the id: " + service.Id);
    
                // ### Running the Service
                //
                // To run the service we call service.StartService().
                // Uou can have a taks that returns when service.Stop() is called.
                Task<bool> serviceStopFuture = service.StartService();
    
                // For the example we use a simple string for the input and output
                // but in the real world it will be some sort of formatted data such as json.
                // The input and output is sent as the data payload of the NATS message.
                byte[] input = Encoding.UTF8.GetBytes("-1,2,100,-2000");
    
                // To "call" a service, we simply make a request to the proper endpoint with
                // data that it expects. Notice how the group name is prepended to the endpoint name.
                Msg minMsg = c.Request("minmax.min", input);
                Console.WriteLine($"Min value is: {Encoding.UTF8.GetString(minMsg.Data)}");

                Msg maxMsg = c.Request("minmax.max", input);
                Console.WriteLine($"Max value is: {Encoding.UTF8.GetString(maxMsg.Data)}");
    
                // The statistics being managed by micro should now reflect the call made
                // to each endpoint, and we didn't have to write any code to manage that.
                EndpointStats esMin = service.GetEndpointStats(min.Name);
                Console.WriteLine($"The min service received {esMin.NumRequests} request(s).");
    
                EndpointStats esMax = service.GetEndpointStats(max.Name);
                Console.WriteLine($"The max service received {esMax.NumRequests} request(s).");
            }
        }
    
        private static void minRequestHandler(IConnection conn, ServiceMsg msg)
        {
            int min = int.MaxValue;
            string[] input = Encoding.UTF8.GetString(msg.Data).Split(',');
            foreach (string n in input) {
                min = Math.Min(min, int.Parse(n));
            }
            msg.Respond(conn, Encoding.UTF8.GetBytes("" + min));
        }
    
        private static void maxRequestHandler(IConnection conn, ServiceMsg msg)
        {
            int max = int.MinValue;
            string[] input = Encoding.UTF8.GetString(msg.Data).Split(',');
            foreach (string n in input) {
                max = Math.Max(max, int.Parse(n));
            }
            msg.Respond(conn, Encoding.UTF8.GetBytes("" + max));
        }
    }
}
