using System;
using System.Net.Sockets;
using System.Text;
using NATS.Client;

namespace IpTests
{
    class Program
    {
        static void Main(string[] args)
        {
            string uri = "nats://localhost:4222";
            if (args.Length == 1)
            {
                uri = args[0];
            }

            Console.WriteLine($"IPv6 supported: {Socket.OSSupportsIPv6}");

            var options = ConnectionFactory.GetDefaultOptions();
            options.Url = uri;

            Console.WriteLine($"Attempting to connect to {uri}");

            try
            {
                var conn = new ConnectionFactory().CreateConnection(options);
                conn.Publish("foo", Encoding.ASCII.GetBytes("Hello World!"));
		        conn.Flush(100);
                Console.WriteLine("Connected! 🙂");
            }
            catch (Exception ex)
            {
                Console.WriteLine("That didn't work 😞");
                Console.WriteLine($"{ex.ToString()}");
            }
           
        }
    }
}
