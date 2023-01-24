using System;
using NATS.Client;

namespace NATSExamples
{
    internal static class JetStreamStarter
    {
        static void Main(string[] args)
        {
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