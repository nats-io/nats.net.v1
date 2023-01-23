using System;
using System.Collections.Generic;
using NATS.Client;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.Service;

namespace NATSExamples
{
    internal static class JetStreamStarter
    {
        static void Main(string[] args)
        {
            List<Endpoint> list = new List<Endpoint>();
            list.Add(new Endpoint("name1", "sub1", "q1", "e1"));
            list.Add(new Endpoint("name2", "sub2", "q2", "e2"));
            SchemaResponse r = 
                new SchemaResponse("id", "name", "version", "desc", list);
            
            Console.WriteLine(r.ToString());
            Console.WriteLine(list[0].ToString());
            if (true) return;

            Options opts = ConnectionFactory.GetDefaultOptions();
            
            opts.Name = "the-client";
            opts.Url = "nats://localhost:4222";
            
            opts.AsyncErrorEventHandler = (obj, a) =>
            {
                Console.WriteLine($"Error: {a.Error}");
            };
            opts.ReconnectedEventHandler = (obj, a) =>
            {
                Console.WriteLine($"Reconnected to {a.Conn.ConnectedUrl}");
            };
            opts.DisconnectedEventHandler = (obj, a) =>
            {
                Console.WriteLine("Disconnected.");
            };
            opts.ClosedEventHandler = (obj, a) =>
            {
                Console.WriteLine("Connection closed.");
            };
            
            Console.WriteLine($"Connecting to '{opts.Url}'");

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                Console.WriteLine("Connected.");
            }
        }
    }
}