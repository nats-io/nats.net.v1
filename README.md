
# NATS - .NET C# Client
A [C# .NET](https://msdn.microsoft.com/en-us/vstudio/aa496123.aspx) client for the [NATS messaging system](https://nats.io).

This Synadia supported client parallels the [NATS GO Client](https://github.com/nats-io/nats).


[![License Apache 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![API Documentation](https://img.shields.io/badge/doc-Doxygen-brightgreen.svg?style=flat)](http://nats-io.github.io/csharp-nats)
[![NuGet](https://img.shields.io/nuget/v/NATS.Client.svg?maxAge=2592000)](https://www.nuget.org/packages/NATS.Client)

## Installation

First, download the source code:
```
git clone git@github.com:nats-io/csharp-nats.git
```

### Quick Start


Then, build the assembly.  

For both .NET 4.5 and .NET core, there are simple batch files.  These will build the respective NATS.Client.dll and the provided examples with only requiring the  .NET framework SDK/Platform

#### .NET 4.5
Ensure you have installed the .NET Framework 4.5.1 or greater.
```
set PATH=C:\Windows\Microsoft.NET\Framework64\v4.0.30319;%PATH%
```
To build simply call:
```
build45.bat
```
The batch file will create a bin directory, and copy all binary files, including samples, into it.
#### .NET core

To build .NET core, you will need version 1.0.0-preview2-003121 or higher, found here:
https://www.microsoft.com/net/core#windows

```
buildcore.bat
```

This will build the .NET core (standard1.6) NATS.Client assembly.  To run the examples, ``cd`` into the directory of the example you wish to run, and use the `dotnet run` command.  e.g.
```
cd examples\Publish
dotnet run
```

### Visual Studio

The recommended alternative is to load `NATSnet45.sln` or `NATSCore.sln` into Visual Studio 2015 to build the version you need.  XML documentation is generated, so code completion, context help, etc, will be available in the editor.  If building .NET core, ensure you have the latest .NET core support for Visual Studio.  Information about that can be found [here](https://blogs.msdn.microsoft.com/visualstudio/2016/06/27/visual-studio-2015-update-3-and-net-core-1-0-available-now/).

#### Project files

The NATS Visual Studio Solution contains several projects, listed below.

* NATS - The NATS.Client assembly
* NATSUnitTests - Visual Studio Unit Tests (ensure you have gnatds.exe in your path to run these).
* Publish Subscribe
  * Publish - A sample publisher.
  * Subscribe - A sample subscriber.
* QueueGroup - An example queue group subscriber.
* Request Reply
  * Requestor - A requestor sample.
  * Replier - A sample replier for the Requestor application.

All examples provide statistics for benchmarking.

### NuGet

The NATS .NET Client can be found in the NuGet Gallery.  It can be found as the [NATS.Client Package](https://www.nuget.org/packages/NATS.Client).

### Building the API Documentation
Doxygen is used for building the API documentation.  To build the API documentation, change directories to `documentation` and run the following command:

```
build_doc.bat
```

Doxygen will build the NATS .NET Client API documentation, placing it in the `documentation\NATS.Client\html` directory.
Doxygen is required to be installed and in the PATH.  Version 1.8 is known to work.

[Current API Documentation](http://nats-io.github.io/csharp-nats)

## Basic Usage

NATS .NET C# Client uses interfaces to reference most NATS client objects, and delegates for all types of events.

### Creating a NATS .NET Application

First, reference the NATS.Client assembly so you can use it in your code.  Be sure to add a reference in your project or if compiling via command line, compile with the /r:NATS.Client.DLL parameter.  While the NATS client is written in C#, any .NET langage can use it.

Below is some code demonstrating basic API usage.  Note that this is example code, not functional as a whole (e.g. requests will fail without a subscriber to reply).

```C#
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// Reference the NATS client.
using NATS.Client;
```

Here are example snippets of using the API to create a connection, subscribe, publish, and request data.

```C#
            // Create a new connection factory to create
            // a connection.
            ConnectionFactory cf = new ConnectionFactory();

            // Creates a live connection to the default
            // NATS Server running locally
            IConnection c = cf.CreateConnection();

            // Setup an event handler to process incoming messages.
            // An anonymous delegate function is used for brevity.
            EventHandler<MsgHandlerEventArgs> h = (sender, args) =>
            {
                // print the message
                Console.WriteLine(args.Message);

                // Here are some of the accessible properties from
                // the message:
                // args.Message.Data;
                // args.Message.Reply;
                // args.Message.Subject;
                // args.Message.ArrivalSubcription.Subject;
                // args.Message.ArrivalSubcription.QueuedMessageCount;
                // args.Message.ArrivalSubcription.Queue;

                // Unsubscribing from within the delegate function is supported.
                args.Message.ArrivalSubcription.Unsubscribe();
            };

            // The simple way to create an asynchronous subscriber
            // is to simply pass the event in.  Messages will start
            // arriving immediately.
            IAsyncSubscription s = c.SubscribeAsync("foo", h);

            // Alternatively, create an asynchronous subscriber on subject foo,
            // assign a message handler, then start the subscriber.   When
            // multicasting delegates, this allows all message handlers
            // to be setup before messages start arriving.
            IAsyncSubscription sAsync = c.SubscribeAsync("foo");
            sAsync.MessageHandler += h;
            sAsync.Start();

            // Simple synchronous subscriber
            ISyncSubscription sSync = c.SubscribeSync("foo");

            // Using a synchronous subscriber, gets the first message available,
            // waiting up to 1000 milliseconds (1 second)
            Msg m = sSync.NextMessage(1000);

            c.Publish("foo", Encoding.UTF8.GetBytes("hello world"));

            // Unsubscribing
            sAsync.Unsubscribe();

            // Publish requests to the given reply subject:
            c.Publish("foo", "bar", Encoding.UTF8.GetBytes("help!"));

            // Sends a request (internally creates an inbox) and Auto-Unsubscribe the
            // internal subscriber, which means that the subscriber is unsubscribed
            // when receiving the first response from potentially many repliers.
            // This call will wait for the reply for up to 1000 milliseconds (1 second).
            m = c.Request("foo", Encoding.UTF8.GetBytes("help"), 1000);

            // Closing a connection
            c.Close();
```
## Basic Encoded Usage
The .NET NATS client mirrors go encoding through serialization and
deserialization.  Simply create an encoded connection and publish
objects, and receive objects through an asynchronous subscription using
the encoded message event handler.  The .NET 4.5 client has a default formatter
serializing objects using the BinaryFormatter, but methods used to serialize and deserialize
objects can be overridden.  The NATS core version does not have serialization
defaults and they must be specified.

```C#
        using (IEncodedConnection c = new ConnectionFactory().CreateEncodedConnection())
        {
            EventHandler<EncodedMessageEventArgs> eh = (sender, args) =>
            {
                // Here, obj is an instance of the object published to
                // this subscriber.  Retrieve it through the
                // ReceivedObject property of the arguments.
                MyObject obj = (MyObject)args.ReceivedObject;

                System.Console.WriteLine("Company: " + obj.Company);
            };

            // Subscribe using the encoded message event handler
            IAsyncSubscription s = c.SubscribeAsync("foo", eh);

            MyObject obj = new MyObject();
            obj.Company = "MyCompany";

            // To publish an instance of your object, simply
            // call the IEncodedConnection publish API and pass
            // your object.
            c.Publish("foo", obj);
            c.Flush();
        }
```

### Other Types of Serialization
Optionally, one can override serialization.  Depending on the level of support or
third party packages used, objects can be serialized to JSON, SOAP, or a custom
scheme.  XML was chosen as the example here as it is natively supported by .NET 4.5.

```C#
        // Example XML serialization.
        byte[] serializeToXML(Object obj)
        {
            MemoryStream  ms = new MemoryStream();
            XmlSerializer x = new XmlSerializer(((SerializationTestObj)obj).GetType());

            x.Serialize(ms, obj);

            byte[] content = new byte[ms.Position];
            Array.Copy(ms.GetBuffer(), content, ms.Position);

            return content;
        }

        Object deserializeFromXML(byte[] data)
        {
            XmlSerializer x = new XmlSerializer(new SerializationTestObj().GetType());
            MemoryStream ms = new MemoryStream(data);
            return x.Deserialize(ms);
        }

        <...>

        // Create an encoded connection and override the OnSerialize and
        // OnDeserialize delegates.
        IEncodedConnection c = new ConnectionFactory().CreateEncodedConnection();
        c.OnDeserialize = deserializeFromXML;
        c.OnSerialize = serializeToXML;

        // From here on, the connection will use the custom delegates
        // for serialization.
```

One can also use `Data Contract` to serialize objects.  Below are simple example
 overrides that work with .NET core:
```C#
[DataContract]
public class JsonObject
{
    [DataMember]
    public string Value = "";
}

internal object jsonDeserializer(byte[] buffer)
{
    using (MemoryStream stream = new MemoryStream())
    {
        var serializer = new DataContractJsonSerializer(typeof(JsonObject));
        stream.Write(buffer, 0, buffer.Length);
        stream.Position = 0;
        return serializer.ReadObject(stream);
    }
}

internal byte[] jsonSerializer(object obj)
{
    if (obj == null)
        return null;

    var serializer = new DataContractJsonSerializer(typeof(JsonObject));

    using (MemoryStream stream = new MemoryStream())
    {
        serializer.WriteObject(stream, obj);
        return stream.ToArray();
    }
}

<...>

// Create an encoded connection and override the OnSerialize and
// OnDeserialize delegates.
IEncodedConnection c = new ConnectionFactory().CreateEncodedConnection();
c.OnDeserialize = jsonDeserializer;
c.OnSerialize = jsonSerializer;

// From here on, the connection will use the custom delegates
// for serialization.
```

## Wildcard Subscriptions

The `*` wildcard matches any token, at any level of the subject:

```c#
IAsyncSubscription s = c.SubscribeAsync("foo.*.baz");
```
This subscriber would receive messages sent to:

* foo.bar.baz
* foo.a.baz
* etc...

It would not, however, receive messages on:

* foo.baz
* foo.baz.bar
* etc...

The `>` wildcard matches any length of the tail of a subject, and can only be the last token.

```c#
IAsyncSubscription s = c.SubscribeAsync("foo.>");
```
This subscriber would receive any message sent to:

* foo.bar
* foo.bar.baz
* foo.foo.bar.bax.22
* etc...

However, it would not receive messages sent on:

* foo
* bar.foo.baz
* etc...

Publishing on this subject would cause the two above subscriber to receive the message:
```c#
c.Publish("foo.bar.baz", null);
```

## Queue Groups

All subscriptions with the same queue name will form a queue group. Each message will be delivered to only one subscriber per queue group, using queue sematics. You can have as many queue groups as you wish. Normal subscribers will continue to work as expected.

```C#
ISyncSubscription s1 = c.SubscribeSync("foo", "job_workers");
```

or

```C#
IAsyncSubscription s = c.SubscribeAsync("foo", "job_workers", myHandler);
```

To unsubscribe, call the ISubscriber Unsubscribe method:
```C#
s.Unsubscribe();
```

When finished with NATS, close the connection.
```C#
c.Close();
```


## Advanced Usage

Connection and Subscriber objects implement IDisposable and can be created in a using statement.  Here is all the code required to connect to a default server, receive ten messages, and clean up, unsubcribing and closing the connection when finished.

```C#
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (ISyncSubscription s = c.SubscribeSync("foo"))
                {
                    for (int i = 0; i < 10; i++)
                    {
                        Msg m = s.NextMessage();
                        System.Console.WriteLine("Received: " + m);
                    }
                }  
            }
```

Or to publish ten messages:

```C#
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                for (int i = 0; i < 10; i++)
                {
                    c.Publish("foo", Encoding.UTF8.GetBytes("hello"));
                }
            }
```

Flush a connection to the server - this call returns when all messages have been processed.  Optionally, a timeout in milliseconds can be passed.

```C#
c.Flush();

c.Flush(1000);
```

Setup a subscriber to auto-unsubscribe after ten messsages.

```C#
        IAsyncSubscription s = c.SubscribeAsync("foo");
        s.MessageHandler += (sender, args) =>
        {
           Console.WriteLine("Received: " + args.Message);
        };

        s.Start();
        s.AutoUnsubscribe(10);
```

Note that an anonymous function was used.  This is for brevity here - in practice, delegate functions can be used as well.  

Other events can be assigned delegate methods through the options object.
```C#
            Options opts = ConnectionFactory.GetDefaultOptions();

            opts.AsyncErrorEventHandler += (sender, args) =>
            {
                Console.WriteLine("Error: ");
                Console.WriteLine("   Server: " + args.Conn.ConnectedUrl);
                Console.WriteLine("   Message: " + args.Error);
                Console.WriteLine("   Subject: " + args.Subscription.Subject);
            };

            opts.ServerDiscoveredEventHandler += (sender, args) =>
            {
                Console.WriteLine("A new server has joined the cluster:");
                Console.WriteLine("    " + String.Join(", ", args.Conn.DiscoveredServers));
            };

            opts.ClosedEventHandler += (sender, args) =>
            {
                Console.WriteLine("Connection Closed: ");
                Console.WriteLine("   Server: " + args.Conn.ConnectedUrl);
            };

            opts.DisconnectedEventHandler += (sender, args) =>
            {
                Console.WriteLine("Connection Disconnected: ");
                Console.WriteLine("   Server: " + args.Conn.ConnectedUrl);
            };

            IConnection c = new ConnectionFactory().CreateConnection(opts);
```

After version 0.5.0, the C# .NET client supports async Requests.

```C#
public async void MyRequestDataMethod(IConnection c)
{
    var m = await c.RequestAsync("foo", null);

    ...
    m = c.RequestAsync("foo", null);
    // do some work
    await m;

    // cancellation tokens are supported.
    var cts = new CancellationTokenSource();

    var msg = c.RequestAsync("foo", null, cts.Token);
    // do stuff
    if (requestIsNowIrrevelant())
        cts.Cancel();

    await msg;
    // be sure to handle OperationCancelled Exception.
}
```

The NATS .NET client supports the cluster discovery protocol.  The list of servers known to a connection is automatically updated when a connection is established, and afterword in realtime as cluster changes occur.  A current list of known servers in a cluster can be obtained using the `IConnection.Servers` property; this list will be used if the client needs to reconnect to the cluster.

## Clustered Usage

```C#
            string[] servers = new string[] {
                "nats://localhost:1222",
                "nats://localhost:1224"
            };

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 1000;
            opts.NoRandomize = true;
            opts.Servers = servers;

            IConnection c = new ConnectionFactory().CreateConnection(opts);
```

## TLS
The NATS .NET client supports TLS 1.2.  Set the secure option, add
the certificate, and connect.  Note that .NET requires both the
private key and certificate to be present in the same certificate file.

```C#
        Options opts = ConnectionFactory.GetDefaultOptions();
        opts.Secure = true;

        // .NET requires the private key and cert in the
        //  same file. 'client.pfx' is generated from:
        //
        // openssl pkcs12 -export -out client.pfx
        //    -inkey client-key.pem -in client-cert.pem
        X509Certificate2 cert = new X509Certificate2("client.pfx", "password");

        opts.AddCertificate(cert);

        IConnection c = new ConnectionFactory().CreateConnection(opts);
```
Many times, it is useful when developing an application (or necessary
when using self-signed certificates) to override server certificate
validation.  This is achieved by overriding the remove certificate
validation callback through the NATS client options.
```C#

    private bool verifyServerCert(object sender,
        X509Certificate certificate, X509Chain chain,
                SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            // Do what is necessary to achieve the level of
            // security you need given a policy error.
        }        

        <...>

        Options opts = ConnectionFactory.GetDefaultOptions();
        opts.Secure = true;
        opts.TLSRemoteCertificationValidationCallback = verifyServerCert;
        opts.AddCertificate("client.pfx");

        IConnection c = new ConnectionFactory().CreateConnection(opts);
```
The NATS server default cipher suites **may not be supported** by the Microsoft
.NET framework.  Please refer to **gnatsd --help_tls** usage and configure
the  NATS server to include the most secure cipher suites supported by the
.NET framework.

## Exceptions

The NATS .NET client can throw the following exceptions:

* NATSException - The generic NATS exception, and base class for all other NATS exception.
* NATSConnectionException - The exception that is thrown when there is a connection error.
* NATSProtocolException -  This exception that is thrown when there is an internal error with the NATS protocol.
* NATSNoServersException - The exception that is thrown when a connection cannot be made to any server.
* NATSSecureConnRequiredException - The exception that is thrown when a secure connection is required.
* NATSConnectionClosedException - The exception that is thrown when a an operation is performed on a connection that is closed.
* NATSSlowConsumerException - The exception that is thrown when a consumer (subscription) is slow.
* NATSStaleConnectionException - The exception that is thrown when an operation occurs on a connection that has been determined to be stale.
* NATSMaxPayloadException - The exception that is thrown when a message payload exceeds what the maximum configured.
* NATSBadSubscriptionException - The exception that is thrown when a subscriber operation is performed on an invalid subscriber.
* NATSTimeoutException - The exception that is thrown when a NATS operation times out.

## Benchmarking the NATS .NET Client

Benchmarking the NATS .NET Client is simple - run the benchmark application with a default NATS server running.  Tests can be customized, run benchmark -h for more details.  In order to get the best out of your test, update the priority of the benchmark application and the NATS server:

```
start /B /REALTIME gnatsd.exe
```
And the benchmark:
```
start /B /REALTIME benchmark.exe
```

The benchmarks include:

* PubOnly<size> - publish only
* PubSub<size> - publish and subscribe
* ReqReply<size> - request/reply
* Lat<size> - latency.

Note, kb/s is solely payload, excluding the NATS message protocol.  Latency is measure in microseconds.

Here is some sample output, from a VM running on a Macbook Pro, simulating a cloud environment.  Running the benchmarks in your environment is *highly* recommended; these numbers below should reflect the low end of performance numbers.
```
PubOnlyNo	  10000000	   4715384 msgs/s	       0 kb/s
PubOnly8b	  10000000	   4058182 msgs/s	   31704 kb/s
PubOnly32b	  10000000	   3044199 msgs/s	   95131 kb/s
PubOnly256b	  10000000	    408034 msgs/s	  102008 kb/s
PubOnly512b	  10000000	    203681 msgs/s	  101840 kb/s
PubOnly1k	   1000000	     94106 msgs/s	   94106 kb/s
PubOnly4k	    500000	     52653 msgs/s	  210612 kb/s
PubOnly8k	    100000	      8552 msgs/s	   68416 kb/s
PubSubNo	  10000000	   1101135 msgs/s	       0 kb/s
PubSub8b	  10000000	   1075814 msgs/s	    8404 kb/s
PubSub32b	  10000000	    990223 msgs/s	   30944 kb/s
PubSub256b	  10000000	    391208 msgs/s	   97802 kb/s
PubSub512b	    500000	    190811 msgs/s	   95405 kb/s
PubSub1k	    500000	     97392 msgs/s	   97392 kb/s
PubSub4k	    500000	     23714 msgs/s	   94856 kb/s
PubSub8k	    100000	     11870 msgs/s	   94960 kb/s
ReqReplNo	     20000	      3245 msgs/s	       0 kb/s
ReqRepl8b	     10000	      3237 msgs/s	      25 kb/s
ReqRepl32b	     10000	      3076 msgs/s	      96 kb/s
ReqRepl256b	      5000	      2446 msgs/s	     611 kb/s
ReqRepl512b	      5000	      2530 msgs/s	    1265 kb/s
ReqRepl1k	      5000	      2973 msgs/s	    2973 kb/s
ReqRepl4k	      5000	      1944 msgs/s	    7776 kb/s
ReqRepl8k	      5000	      1394 msgs/s	   11152 kb/s
LatNo (us)	500 msgs, 141.74 avg, 86.00 min, 600.40 max, 23.28 stddev
Lat8b (us)	500 msgs, 141.52 avg, 70.30 min, 307.00 max, 26.77 stddev
Lat32b (us)	500 msgs, 139.64 avg, 93.70 min, 304.20 max, 15.88 stddev
Lat256b (us)	500 msgs, 175.55 avg, 101.80 min, 323.90 max, 14.93 stddev
Lat512b (us)	500 msgs, 182.56 avg, 103.00 min, 1982.00 max, 81.50 stddev
Lat1k (us)	500 msgs, 193.15 avg, 86.20 min, 28705.20 max, 1277.13 stddev
Lat4k (us)	500 msgs, 291.09 avg, 99.70 min, 43679.10 max, 2047.67 stddev
Lat8k (us)	500 msgs, 363.56 avg, 131.50 min, 39428.50 max, 1990.81 stddev
```

## About the code and contributing

A note:  The NATS C# .NET client was originally developed with the idea in mind that it would support the .NET 4.0 code base for increased adoption, and closely parallel the GO client (internally) for maintenance purposes.  So, some of the nice .NET APIs/features were intentionally left out.  While this has certainly paid off, after consideration, and some maturation of the NATS C# library, the NATS C# code will move toward more idiomatic .NET coding style where it makes sense.

To that end, with any contributions, certainly feel free to code in a more .NET idiomatic style than what you see.  PRs are always welcome!

## TODO
* [ ] Another performance pass - look at stream directly over socket, contention, fastpath optimizations, rw locks.
* [ ] Rx API (unified over NATS Streaming?)
* [ ] Expand Unit Tests to test internals (namely Parsing)
* [ ] WCF bindings (If requested for legacy, or user contributed)
* [X] Travis CI (Used AppVeyor instead)
* [ ] Allow configuration for performance tuning (buffer sizes), defaults based on plaform.
* [X] [.NET Core](https://github.com/dotnet/core) compatibility, TLS required.
* [ ] Azure Service Bus Connector
* [ ] Visual Studio [Starter Kit](https://msdn.microsoft.com/en-us/library/ccd9ychb.aspx)
* [X] Convert unit tests to xunit
* [X] Comprehensive benchmarking
* [X] TLS
* [X] Encoding (Serialization/Deserialization)
* [X] Update delegates from traditional model to custom
* [X] NuGet package
* [X] Strong name the assembly

Any suggestions and/or contributions are welcome!