using System;
using NATS.Client;

namespace NATSExamples
{
    internal static class JetStreamStarter
    {
        static void Main(string[] args)
        {
            Test("1.2.3.4:4222");
            Test("host:4222");
            Test("nats://");
            Test("host");
            Test("1.2.3.4");
            Test("x:p@host");
            Test("x:p@1.2.3.4");
            Test("x:4222");
            Test("x:p@host:4222");
            Test("x:p@1.2.3.4:4222");
            Test("nats://u:p@");
            Test("nats://:4222");
            Test("nats://u:p@:4222");
            Test("nats://host");
            Test("nats://u:p@host");
            Test("nats://1.2.3.4");
            Test("nats://u:p@1.2.3.4");
            Test("nats://host:4222");
            Test("nats://u:p@host:4222");
            Test("nats://1.2.3.4:4222");
            Test("nats://u:p@1.2.3.4:4222");
        }

        static void Testx(string s)
        {
            try
            {
                NatsUri uri = new NatsUri(s);
                Console.WriteLine($"{s} --> scheme:'{uri.Scheme}', host:'{uri.Host}', up:'{uri.UserInfo}', port:{uri.Port}");
                // Console.WriteLine($"{s} --> '{uri}'");
            }
            catch (Exception e)
            {
                Console.WriteLine(s + " --> " + e.GetType() + " | " + e.Message);
            }
        }
        
        static void Test(string s)
        {
            try
            {
                Uri uri = new Uri(s);
                Console.WriteLine($"{s} --> scheme:'{uri.Scheme}', host:'{uri.Host}', up:'{uri.UserInfo}', port:{uri.Port}, path:'{uri.LocalPath}'");
                // Console.WriteLine($"{s} --> '{uri}'");
            }
            catch (Exception e)
            {
                Console.WriteLine(s + " xxx " + e.GetType() + " | " + e.Message);
            }

        }
    }
}
/*
test string --> result of new Uri(String)

[2] just scheme --> throw
proto:// --> scheme:'proto', host:'', up:'', port:-1, path:'/'

[5] has scheme, null host, non-null path --> rethrow
proto://u:p@ --> System.UriFormatException Invalid URI: Invalid port specified.
proto://:4222 xxx System.UriFormatException Invalid URI: The hostname could not be parsed.
proto://u:p@:4222 xxx System.UriFormatException Invalid URI: The hostname could not be parsed.

[6] has scheme and host just needs port
proto://host        --> scheme:'proto', host:'host', up:'null', port:-1, path:'/'
proto://u:p@host    --> scheme:'proto', host:'host', up:'u:p', port:-1, path:'/'
proto://1.2.3.4     --> scheme:'proto', host:'1.2.3.4', up:'null', port:-1, path:'/'
proto://u:p@1.2.3.4 --> scheme:'proto', host:'1.2.3.4', up:'u:p', port:-1, path:'/'

[7] has scheme, host and port
proto://host:4222        --> scheme:'proto', host:'host', up:'', port:4222, path:'/'
proto://u:p@host:4222    --> scheme:'proto', host:'host', up:'u:p', port:4222, path:'/'
proto://1.2.3.4:4222     --> scheme:'proto', host:'1.2.3.4', up:'', port:4222, path:'/'
proto://u:p@1.2.3.4:4222 --> scheme:'proto', host:'1.2.3.4', up:'u:p', port:4222, path:'/'
*/