
# NATS - .NET C# Client
A [C# .NET](https://msdn.microsoft.com/en-us/vstudio/aa496123.aspx) client for the [NATS messaging system](https://nats.io).

This is an alpha release, based on the [NATS GO Client](https://github.com/nats-io/nats).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)

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

            // Simple asynchronous subscriber on subject foo
            IAsyncSubscription sAsync = c.SubscribeAsync("foo");

            // Assign a message handler.  An anonymous delegate function
            // is used for brevity.
            sAsync.MessageHandler += (sender, msgArgs) =>
            {
                // print the message
                Console.WriteLine(msgArgs.Message);

                // Here are some of the accessible properties from
                // the message:
                // msgArgs.Message.Data;
                // msgArgs.Message.Reply;
                // msgArgs.Message.Subject;
                // msgArgs.Message.ArrivalSubcription.Subject;
                // msgArgs.Message.ArrivalSubcription.QueuedMessageCount;
                // msgArgs.Message.ArrivalSubcription.Queue;

                // Unsubscribing from within the delegate function is supported.
                msgArgs.Message.ArrivalSubcription.Unsubscribe();
            };

            // Start the subscriber.  Asycnronous subscribers have a
            // method to start receiving data.  This allows the message handler
            // to be setup before data starts arriving; important if multicasting
            // delegates.
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
IAsyncSubscription s = c.SubscribeAsync("foo", "job_workers");
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
Known Issues
* Some unit tests are incomplete or fail.  This is due to long connect times with the underlying .NET TCPClient API, issues with the tests themselves, or bugs (This IS an alpha).
* There can be an issue with a flush hanging in some situations.  I'm looking into it.

TODO
* API documentation
* WCF bindings
* Strong name the assembly


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


