
# NATS - .NET C# Client
A [C# .NET](https://msdn.microsoft.com/en-us/vstudio/aa496123.aspx) client for the [NATS messaging system](https://nats.io).

This is an beta release, based on the [NATS GO Client](https://github.com/nats-io/nats).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![API Documentation](https://img.shields.io/badge/doc-Doxygen-brightgreen.svg?style=flat)](http://nats-io.github.io/cnats)

## Installation

First, download the source code:
```
git clone git@github.com:nats-io/csnats.git .
```

### Quick Start

Ensure you have installed the .NET Framework 4.0 or greater.  Set your path to include csc.exe, e.g.
```
set PATH=C:\Windows\Microsoft.NET\Framework64\v4.0.30319;%PATH%
```
Then, build the assembly.  There is a simple batch file, build.bat, that will build the assembly (NATS.Client.dll) and the provided examples with only requriing the  .NET framework SDK.

```
build.bat
```
The batch file will create a bin directory, and copy all binary files, including samples, into it.

### Visual Studio

The recommended alternative is to load NATS.sln into Visual Studio 2013 Express or better.  Later versions of Visual Studio should automatically upgrade the solution and project files for you.  XML documenation is generated, so code completion, context help, etc, will be available in the editor.


#### Project files

The NATS Visual Studio Solution contains several projects, listed below.

* NATS - The NATS.Client assembly
* NATSUnitTests - Visual Studio Unit Tests (ensure you have gnatds.exe in your path for these).
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

[Current API Documentation](http://nats-io.github.io/cnats)

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
objects, and receive objects through an asyncronous subscription using
the encoded message event handler.  By default, objects are serialized
using the BinaryFormatter, but methods used to serialize and deserialize
objects can be overridden.

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
            obj.Company = "Apcera";
            
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
scheme.  XML was chosen as the example here as it is natively supported 
in all versions of .NET.

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

The `>` wildcard matches any length of the fail of a subject, and can only be the last token.

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

## Miscellaneous 
###Known Issues
* In the original v0.1-alpha release, there was an issue with a flush hanging in some situations.  This has been fixed in the v0.2-alpha release.

###TODO
* [ ] WCF bindings
* [X] TLS
* [X] Encoding (Serialization/Deserialization)
* [X] Update delegates from traditional model to custom
* [X] NuGet package
* [X] Strong name the assembly

## License

(The MIT License)

Copyright (c) 2012-2015 Apcera Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.


