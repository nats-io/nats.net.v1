![NATS](https://raw.githubusercontent.com/nats-io/nats.net/master/documentation/large-logo.png)

# NATS - .NET C# Client

A [C# .NET](https://msdn.microsoft.com/en-us/vstudio/aa496123.aspx) client for the [NATS messaging system](https://nats.io) multi targetting `.NET4.6+` and `.NETStandard1.6`.

This NATS Maintainer supported client parallels the [NATS GO Client](https://github.com/nats-io/nats).

[![License Apache 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![API Documentation](https://img.shields.io/badge/doc-Doxygen-brightgreen.svg?style=flat)](http://nats-io.github.io/nats.net)
[![Build Status](https://dev.azure.com/NATS-CI/NATS-CI/_apis/build/status/nats-io.nats.net-CI?branchName=master)](https://dev.azure.com/NATS-CI/NATS-CI/_build/latest?definitionId=2&branchName=master)
[![NuGet](https://img.shields.io/nuget/v/NATS.Client.svg?cacheSeconds=3600)](https://www.nuget.org/packages/NATS.Client)

## Getting started
The easiest and recommended way to start using NATS in your .NET projects, is to use the [NuGet package]((https://www.nuget.org/packages/NATS.Client)). For examples on how to use the client, see below or in any of the included sample projects.

## Get up and running with the source code
First, download the source code:

```text
git clone git@github.com:nats-io/nats.net.git
```

### Project files

The repository contains several projects, all located under `src\`

* NATS - The NATS.Client assembly
* Tests
    * IntegrationTests - XUnit tests, verifying the client integration with `nats-server.exe` (ensure you have `nats-server.exe` in your path to run these).
    * IntegrationTestsInternal - XUnit tests, verifying internal only functionality
    * IntegrationTestsUsingNitoAsyncEx - XUnit tests, verifying functionality that depends on NitoAsyncEx 
    * UnitTests - XUnit tests that requires no dependencies
* Samples
    
    To see, the full set of samples is documented in the [Samples Readme](src/Samples/README.md). Some examples provide statistics for benchmarking.

### .NET Core SDK
.NET Core SDK style projects are used, so ensure your environment (command line, VSCode, Visual Studio, etc) supports the targetted .NET Core SDK in `src\global.json` as well as .NET Framework 4.6 or greater.

### Visual Studio
The recommendation is to load `src\NATS.sln` into Visual Studio 2019 (Visual Studio 2017 works as well). .NET Core SDK style projects are used to multitarget different frameworks, so when working with the source code (debugging, running tets etc) you might need to mind the "context" of the current framework.

XML documentation is generated (in `Release`), so code completion, context help, etc, will be available in the editor.

### Command line
Since .NET Core SDK style projects are used, you can use the .NET SDK to build, run tests, pack etc.

E.g. to [build](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-build):

```text
dotnet build src\NATS.sln -c Release
```

This will build the respective NATS.Client.dll, examples etc in Release mode, with only requiring the .NET Core SDK and the .NET Platform.

### Building the API Documentation

Doxygen is used for building the API documentation.  To build the API documentation, change directories to `documentation` and run the following command:

```text
documentation\build_doc.bat
```

Doxygen will build the NATS .NET Client API documentation, placing it in the `documentation\NATS.Client\html` directory.
Doxygen is required to be installed and in the PATH.  Version 1.8 is known to work.

[Current API Documentation](http://nats-io.github.io/nats.net)

## BETA / Experimental News

### Simplification

There is a new simplified api that makes working with streams and consumers well, simpler!

Check out the examples:

* [SimplificationContextExample](src/Samples/SimplificationContext/SimplificationContextExample.cs)
* [FetchBytesExample](src/Samples/SimplificationFetchBytes/FetchBytesExample.cs)
* [FetchMessagesExample](src/Samples/SimplificationFetchMessages/FetchMessagesExample.cs)
* [IterableConsumerExample](src/Samples/SimplificationIterableConsumer/IterableConsumerExample.cs)
* [MessageConsumerExample](src/Samples/SimplificationMessageConsumer/MessageConsumerExample.cs)
* [NextExample](src/Samples/SimplificationNext/NextExample.cs)

### Service Framework

The service API allows you to easily build NATS services The services API is currently in beta functionality.

The Services Framework introduces a higher-level API for implementing services with NATS. NATS has always been a strong technology on which to build services, as they are easy to write, are location and DNS independent and can be scaled up or down by simply adding or removing instances of the service.

The Services Framework further streamlines their development by providing observability and standardization. The Service Framework allows your services to be discovered, queried for status and schema information without additional work.

Check out the [ServiceExample](src/Samples/ServiceExample/ServiceExample.cs)

## Version Notes

### Version 1.0.5 Max Payload Check

As of version 1.0.5, there is no longer client side checking 
1. that a message payload is less than the server configuration (Core and JetStream publishes)
2. is less than the stream configuration (JetStream publishes)

Please see unit test for examples of this behavior. 
`TestMaxPayload` in [TestBasic](src/Tests/IntegrationTests/TestBasic.cs)
and
`TestMaxPayloadJs` in [TestJetStream](src/Tests/IntegrationTests/TestJetStream.cs)

### Version 1.0.1 Consumer Create

This release by default will use a new JetStream consumer create API when interacting with nats-server version 2.9.0 or higher.
This changes the subjects used by the client to create consumers, which might in some cases require changes in access and import/export configuration.
The developer can opt out of using this feature by using a custom JetStreamOptions and using it when creating
JetStream, Key Value and Object Store regular and management contexts.

```csharp
JetStreamOptions jso = JetStreamOptions.Builder().WithOptOut290ConsumerCreate(true).Build();

IJetStream js = connection.CreateJetStreamContext(jso);
IJetStreamManagement jsm = connection.CreateJetStreamManagementContext(jso);
IKeyValue kv = connection.CreateKeyValueContext("bucket", KeyValueOptions.Builder(jso).Build());
IKeyValueManagement kvm = connection.CreateKeyValueManagementContext(KeyValueOptions.Builder(jso).Build());
IObjectStore os = connection.CreateObjectStoreContext("bucket", ObjectStoreOptions.Builder(jso).Build());
IObjectStoreManagement osm = connection.CreateObjectStoreManagementContext(ObjectStoreOptions.Builder(jso).Build());
```

## Basic Usage

NATS .NET C# Client uses interfaces to reference most NATS client objects, and delegates for all types of events.

### Creating a NATS .NET Application

First, reference the NATS.Client assembly so you can use it in your code.  Be sure to add a reference in your project or if compiling via command line, compile with the /r:NATS.Client.DLL parameter.  While the NATS client is written in C#, any .NET langage can use it.

Below is some code demonstrating basic API usage.  Note that this is example code, not functional as a whole (e.g. requests will fail without a subscriber to reply).

```c#
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// Reference the NATS client.
using NATS.Client;
```

Here are example snippets of using the API to create a connection, subscribe, publish, and request data.

```c#
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

// Draining and closing a connection
c.Drain();

// Closing a connection
c.Close();
```

## RX Usage
Importing the namespace `NATS.Client.Rx` you will be able to use an extension method
`connection.Observe(subject)` to turn the connection to an observable. If you have a more
advanced `IAsyncSubscription`, you can use `asyncSubscription.ToObservable()`.

You can now import the namespace `NATS.Client.Rx.Ops`. After this you get builtin support for:
- Subscribe
- SubscribeSafe (will not fail an observer if it misbehaves)
- Where
- Select

If you want, you could instead take an external dependency on `System.Reactive` and use that
instead of `NATS.RX.Ops`.

See the full example here: [RxSample](src/Samples/RxSample/RxSample.cs)

## Basic Encoded Usage
The .NET NATS client mirrors go encoding through serialization and
deserialization.  Simply create an encoded connection and publish
objects, and receive objects through an asynchronous subscription using
the encoded message event handler.  The .NET 4.6 client has a default formatter
serializing objects using the BinaryFormatter, but methods used to serialize and deserialize
objects can be overridden.  The NATS core version does not have serialization
defaults and they must be specified.

```c#
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
scheme.  XML was chosen as the example here as it is natively supported by .NET 4.6.

```c#
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

```c#
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

```c#
ISyncSubscription s1 = c.SubscribeSync("foo", "job_workers");
```

or

```c#
IAsyncSubscription s = c.SubscribeAsync("foo", "job_workers", myHandler);
```

To unsubscribe, call the ISubscriber Unsubscribe method:
```c#
s.Unsubscribe();
```

When finished with NATS, close the connection.
```c#
c.Close();
```


## Advanced Usage

Connection and Subscriber objects implement IDisposable and can be created in a using statement.  Here is all the code required to connect to a default server, receive ten messages, and clean up, unsubcribing and closing the connection when finished.

```c#
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

```c#
using (IConnection c = new ConnectionFactory().CreateConnection())
{
    for (int i = 0; i < 10; i++)
    {
        c.Publish("foo", Encoding.UTF8.GetBytes("hello"));
    }
}
```

Flush a connection to the server - this call returns when all messages have been processed.  Optionally, a timeout in milliseconds can be passed.

```c#
c.Flush();

c.Flush(1000);
```

Setup a subscriber to auto-unsubscribe after ten messsages.

```c#
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
```c#
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

```c#
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

```c#
string[] servers = new string[] {
    "nats://localhost:1222",
    "nats://localhost:1224"
};

Options opts = ConnectionFactory.GetDefaultOptions();
opts.MaxReconnect = 2;
opts.ReconnectWait = 1000;
opts.Servers = servers;

IConnection c = new ConnectionFactory().CreateConnection(opts);
```

## TLS

The NATS .NET client supports TLS 1.2.  Set the secure option, add
the certificate, and connect.  Note that .NET requires both the
private key and certificate to be present in the same certificate file.

In addition to the code found here, please refer to the sample code at
[TlsVariationsExample](src/Samples/TlsVariationsExample/TlsVariationsExample.cs)

```c#
Options opts = ConnectionFactory.GetDefaultOptions();
opts.Secure = true;

// .NET requires the private key and cert in the
//  same file. 'client.pfx' is generated from:
//
// openssl pkcs12 -export -out client.pfx
//    -inkey client-key.pem -in client-cert.pem
X509Certificate2 cert = new X509Certificate2("client.pfx", "password");

opts.AddCertificate(cert);

// Some connections like those with OCSP 
// require CheckCertificateRevocation
opts.CheckCertificateRevocation = true;

IConnection c = new ConnectionFactory().CreateConnection(opts);
```

Many times, it is useful when developing an application (or necessary
when using self-signed certificates) to override server certificate
validation.  This is achieved by overriding the remove certificate
validation callback through the NATS client options.

```c#
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
.NET framework.  Please refer to **nats-server --help_tls** usage and configure
the  NATS server to include the most secure cipher suites supported by the
.NET framework.

## Custom Dialer/Custom TCP connection. 

The NATs .NET client supports passing in a custom implementation of the [ITCPConnection](TCPConnCustom/src/NATS.Client/ITCPConnection.cs) class.

```c#
	public class TCPConnection : ITCPConnection
    {
        <Custom implementation of ITCPConnection>
    }

	<...>
		Options opts = ConnectionFactory.GetDefaultOptions();
        opts.TCPConnection = new CustomTCPConnection();

        IConnection c = new ConnectionFactory().CreateConnection(opts);
```

This is useful for testing, or implementing a TCPConnection that supports TLS termination.

See [TLSReverseProxyExample](src\Samples\TLSReverseProxyExample) for an implementation. 

## NATS 2.0 Authentication (Nkeys and User Credentials)

This requires server with version >= 2.0.0

NATS servers have a new security and authentication mechanism to authenticate with user credentials and Nkeys. The simplest form is to use the helper method UserCredentials(credsFilepath).

```c#
IConnection c = new ConnectionFactory().CreateConnection("nats://127.0.0.1", "user.creds")
```

The helper methods creates two callback handlers to present the user JWT and sign the nonce
challenge from the server. The core client library never has direct access to your private
key and simply performs the callback for signing the server challenge. The helper will load
and wipe and erase memory it uses for each connect or reconnect.

The helper also can take two entries, one for the JWT and one for the NKey seed file.

```c#
IConnection c = new ConnectionFactory().CreateConnection("nats://127.0.0.1", "user.jwt", "user.nk");
```

You can also set the event handlers directly and manage challenge signing directly.

```c#
EventHandler<UserJWTEventArgs> jwtEh = (sender, args) =>
{
    // Obtain a user JWT...
    string jwt = getMyUserJWT();

    // You must set the JWT in the args to hand off
    // to the client library.
    args.JWT = jwt;
};

EventHandler<UserSignatureEventArgs> sigEh = (sender, args) =>
{
    // get a private key seed from your environment.
    string seed = getMyUserSeed();

    // Generate a NkeyPair
    NkeyPair kp = Nkeys.FromSeed(seed);

    // Sign the nonce and return the result in the SignedNonce
    // args property.  This must be set.
    args.SignedNonce = kp.Sign(args.ServerNonce)
};
Options opts = ConnectionFactory.GetDefaultOptions();
opts.SetUserCredentialHandlers(jwtEh, sigEh);

IConnection c = new ConnectionFactory().CreateConnection(opts));
```

Bare Nkeys are also supported. The nkey seed should be in a read only file, e.g. seed.txt

```bash
> cat seed.txt
SUAGMJH5XLGZKQQWAWKRZJIGMOU4HPFUYLXJMXOO5NLFEO2OOQJ5LPRDPM
```

This is a helper function which will load and decode and do the proper signing for the
server nonce.  It will clear memory in between invocations. You can choose to use the
low level option and provide the public key and a signature callback on your own.

```c#
Options opts = ConnectionFactory.GetDefaultOptions();
opts.SetNkey("UCKKTOZV72L3NITTGNOCRDZUI5H632XCT4ZWPJBC2X3VEY72KJUWEZ2Z",
"./config/certs/user.nk");

// Direct
IConnection c = new ConnectionFactory().CreateConnection(opts));
```

## Message Headers

The NATS.Client version 0.11.0 and NATS server version 2.2 support message headers.
Message headers are represented as a string name value pair just as HTTP headers are.

### Setting Message Headers

```c#
IConnection c = new new ConnectionFactory().CreateConnection();

Msg m = new Msg();
m.Header["Content-Type"] = "json";
m.Subject = "foo";
c.Publish(m);
```

### Getting Message Headers

```c#
IConnection c = new new ConnectionFactory().CreateConnection();
var s = c.SubscribeSync("foo")

Msg m = s.NextMessage();
string contentType = m.Header["Content-Type"];
```

## Exceptions

The NATS .NET client can throw the following exceptions:

* NATSException - The generic NATS exception, and base class for all other NATS exception.
* NATSConnectionException - The exception that is thrown when there is a connection error.
* NATSProtocolException -  This exception that is thrown when there is an internal error with the NATS protocol.
* NATSNoServersException - The exception that is thrown when a connection cannot be made to any server.
* NATSSecureConnRequiredException - The exception that is thrown when a secure connection is required.
* NATSConnectionClosedException - The exception that is thrown when an operation is performed on a connection that is closed.
* NATSSlowConsumerException - The exception that is thrown when a consumer (subscription) is slow.
* NATSStaleConnectionException - The exception that is thrown when an operation occurs on a connection that has been determined to be stale.
* NATSMaxPayloadException - The exception that is thrown when a message payload exceeds what the maximum configured.
* NATSBadSubscriptionException - The exception that is thrown when a subscriber operation is performed on an invalid subscriber.
* NATSTimeoutException - The exception that is thrown when a NATS operation times out.
* NATSJetStreamStatusException - The exception that is thrown when a JetStream subscription detects an exceptional or unknown status
 
## JetStream

Publishing and subscribing to JetStream enabled servers is straightforward.  A
JetStream enabled application will connect to a server, establish a JetStream
context, and then publish or subscribe.  This can be mixed and matched with standard
NATS subject, and JetStream subscribers, depending on configuration, receive messages
from both streams and directly from other NATS producers.

### The JetStream Context

After establishing a connection as described above, create a JetStream Context.

```c#
IJetStream js = c.CreateJetStreamContext();
```

You can pass options to configure the JetStream client, although the defaults should
suffice for most users.  See the `JetStreamOptions` class.

There is no limit to the number of contexts used, although normally one would only
require a single context.  Contexts may be prefixed to be used in conjunction with
NATS authorization.

### Publishing

To publish messages, use the `IJetStream.Publish(...)` API.  A stream must be established
before publishing. You can publish in either a synchronous or asynchronous manner.

**Synchronous:**

```c#
// create a typical NATS message
Msg msg = new Msg("foo", Encoding.UTF8.GetBytes("hello"));
PublishAck pa = js.Publish(msg);
```

See `JetStreamPublish` in the JetStream samples for a detailed and runnable sample.

If there is a problem an exception will be thrown, and the message may not have been
persisted.  Otherwise, the stream name and sequence number is returned in the publish
acknowledgement.

There are a variety of publish options that can be set when publishing.  When duplicate
checking has been enabled on the stream, a message ID should be set. One set of options
are expectations.  You can set a publish expectation such as a particular stream name,
previous message ID, or previous sequence number.  These are hints to the server that
it should reject messages where these are not met, primarily for enforcing your ordering
or ensuring messages are not stored on the wrong stream.

The PublishOptions are immutable, but the builder can be re-used for expectations by clearing the expected.

For example:

```c#
PublishOptions.PublishOptionsBuilder builder = PublishOptions.Builder()
  .WithExpectedStream(stream)
  .WithMessageId("mid1");

PublishAck pa = js.Publish("foo", null, builder.Build());

pubOptsBuilder.ClearExpected()
  .WithExpectedLastSequence("mid1")
  .WithMessageId("mid2");
pa = js.Publish("foo", null, pubOptsBuilder.build());
```

See `JetStreamPublishWithOptionsUseCases` in the JetStream samples for a detailed and runnable sample.

**Asynchronous:**

```c#
IList<Task<PublishAck>> tasks = new new List<Task<PublishAck>>();
for (int x = 1; x < roundCount; x++) {
  // create a typical NATS message
  Msg msg = new Msg("foo", Encoding.UTF8.GetBytes("hello"));

  // Publish a message
  tasks.Add(js.PublishAsync(msg));
}

foreach (Task<PublishAck> task in tasks) {
 ... process the task
}
```

See the `JetStreamPublishAsync` in the JetStream samples for a detailed and runnable sample.

#### ReplyTo When Publishing

The Message object allows you to set a replyTo, but in publish requests,
the replyTo is reserved for internal use as the address for the
server to respond to the client with the PublishAck.

### Subscribing

There are three methods of subscribing, **Push Async**, **Push Sync** and **Pull** with each variety having its own set of options and abilities.

Push subscriptions can be synchronous or asynchronous. The server *pushes* messages to the client.

### Push Async Subscribing

```c#
void MyHandler(object sender, MsgHandlerEventArgs args)
{
    // Process the message.
    // Ack the message depending on the ack model
}

PushSubscribeOptions pso = PushSubscribeOptions.Builder()
    .WithDurable("optional-durable-name")
    .build();

boolean autoAck = ...

// Subscribe using the handler
IJetStreamPushAsyncSubscription sub = 
    js.PushSubscribeAsync("subject", MyHandler, false, pso);
```

See the `JetStreamPushSubcribeBasicAsync` in the JetStream samples for a detailed and runnable sample.

### Push Sync Subscribing

```c#
PushSubscribeOptions pso = PushSubscribeOptions.Builder()
    .WithDurable("optional-durable-name")
    .build();

// Subscribe 
IJetStreamPushSyncSubscription sub = 
        js.PushSubscribeSync("subject", pso);
```

See `JetStreamPushSubcribeSync` in the JetStream samples for a detailed and runnable sample.

### Pull Subscribing

Pull subscriptions are always synchronous. The server organizes messages into a batch
which it sends when requested.

```c#
    PullSubscribeOptions options = PullSubscribeOptions.Builder()
        .WithDurable("durable-name-is-required")
        .Build();

    IJetStreamPullSubscription sub = js.PullSubscribe("subject", options);
```

**Fetch:**

```c#
List<Msg> message = sub.Fetch(100, 1000); // 100 messages, 1000 millis timeout
for (Msg m : messages) {
    // process message
    m.Ack();
}
```

The fetch pull is a *macro* pull that uses advanced pulls under the covers to return a list of messages.
The list may be empty or contain at most the batch size. All status messages are handled for you.
The client can provide a timeout to wait for the first message in a batch.
The fetch call returns when the batch is ready.
The timeout may be exceeded if the server sent messages very near the end of the timeout period.

See `JetStreamPullSubFetch` and `JetStreamPullSubFetchUseCases`
in the JetStream samples for a detailed and runnable sample.

**Batch Size:**

```c#
sub.Pull(100); // 100 messages
...
Msg m = sub.NextMessage(1000);
```

An advanced version of pull specifies a batch size. When asked, the server will send whatever
messages it has up to the batch size. If it has no messages it will wait until it has some to send.
The client may time out before that time. If there are less than the batch size available,
you can ask for more later. Once the entire batch size has been filled, you must make another pull request.

See `JetStreamPullSubBatchSize` and `JetStreamPullSubBatchSizeUseCases`
in the JetStream samples for detailed and runnable samples.

**No Wait and Batch Size:**

```c#
sub.PullNoWait(100); // 100 messages
...
Msg m = sub.NextMessage(1000);
```

An advanced version of pull also specifies a batch size. When asked, the server will send whatever
messages it has up to the batch size, but will never wait for the batch to fill and the client
will return immediately. If there are less than the batch size available, you will get what is
available and a 404 status message indicating the server did not have enough messages.
You must make a pull request every time. **This is an advanced api**

See the `JetStreamPullSubNoWaitUseCases` in the JetStream samples for a detailed and runnable sample.

**Expires In and Batch Size:**

```c#
sub.PullExpiresIn(100, 3000); // 100 messages, expires in 3000 millis
...
Msg m = sub.NextMessage(4000);
```

Another advanced version of pull specifies a maximum time to wait for the batch to fill.
The server returns messages until either the batch is filled or the time expires. It's important to
set your client's timeout to be longer than the time you've asked the server to expire in.
You must make a pull request every time. In subsequent pulls, you will receive multiple 408 status
messages, one for each message the previous batch was short. You can just ignore these.
**This is an advanced api**

See `JetStreamPullSubExpiresIn` and `JetStreamPullSubExpiresInUseCases`
in the JetStream samples for detailed and runnable samples.

### Client Error Messages

In addition to some generic validation messages for values in builders, there are also additional grouped and numbered client error messages:  
* Subscription building and creation
* Consumer creation
* Object Store operations

| Name                                         | Group | Code  | Description                                                                                         |
|----------------------------------------------|-------|-------|-----------------------------------------------------------------------------------------------------|
| JsSoDurableMismatch                          | SO    | 90101 | Builder durable must match the consumer configuration durable if both are provided.                 |
| JsSoDeliverGroupMismatch                     | SO    | 90102 | Builder deliver group must match the consumer configuration deliver group if both are provided.     |
| JsSoDeliverSubjectMismatch                   | SO    | 90103 | Builder deliver subject must match the consumer configuration deliver subject if both are provided. |
| JsSoOrderedNotAllowedWithBind                | SO    | 90104 | Bind is not allowed with an ordered consumer.                                                       |
| JsSoOrderedNotAllowedWithDeliverGroup        | SO    | 90105 | Deliver group is not allowed with an ordered consumer.                                              |
| JsSoOrderedNotAllowedWithDurable             | SO    | 90106 | Durable is not allowed with an ordered consumer.                                                    |
| JsSoOrderedNotAllowedWithDeliverSubject      | SO    | 90107 | Deliver subject is not allowed with an ordered consumer.                                            |
| JsSoOrderedRequiresAckPolicyNone             | SO    | 90108 | Ordered consumer requires Ack Policy None.                                                          |
| JsSoOrderedRequiresMaxDeliver                | SO    | 90109 | Max deliver is limited to 1 with an ordered consumer.                                               |
| JsSoNameMismatch                             | SO    | 90110 | Builder name must match the consumer configuration name if both are provided.                       |
| JsSoOrderedMemStorageNotSuppliedOrTrue       | SO    | 90111 | Mem Storage must be true if supplied.                                                               |
| JsSoOrderedReplicasNotSuppliedOrOne          | SO    | 90112 | Replicas must be 1 if supplied.                                                                     |
| JsSubPullCantHaveDeliverGroup                | SUB   | 90001 | Pull subscriptions can't have a deliver group.                                                      |
| JsSubPullCantHaveDeliverSubject              | SUB   | 90002 | Pull subscriptions can't have a deliver subject.                                                    |
| JsSubPushCantHaveMaxPullWaiting              | SUB   | 90003 | Push subscriptions cannot supply max pull waiting.                                                  |
| JsSubQueueDeliverGroupMismatch               | SUB   | 90004 | Queue / deliver group mismatch.                                                                     |
| JsSubFcHbNotValidPull                        | SUB   | 90005 | Flow Control and/or heartbeat is not valid with a pull subscription.                                |
| JsSubFcHbNotValidQueue                       | SUB   | 90006 | Flow Control and/or heartbeat is not valid in queue mode.                                           |
| JsSubNoMatchingStreamForSubject              | SUB   | 90007 | No matching streams for subject.                                                                    |
| JsSubConsumerAlreadyConfiguredAsPush         | SUB   | 90008 | Consumer is already configured as a push consumer.                                                  |
| JsSubConsumerAlreadyConfiguredAsPull         | SUB   | 90009 | Consumer is already configured as a pull consumer.                                                  |
| _removed_                                    | SUB   | 90010 |                                                                                                     |
| JsSubSubjectDoesNotMatchFilter               | SUB   | 90011 | Subject does not match consumer configuration filter.                                               |
| JsSubConsumerAlreadyBound                    | SUB   | 90012 | Consumer is already bound to a subscription.                                                        |
| JsSubExistingConsumerNotQueue                | SUB   | 90013 | Existing consumer is not configured as a queue / deliver group.                                     |
| JsSubExistingConsumerIsQueue                 | SUB   | 90014 | Existing consumer  is configured as a queue / deliver group.                                        |
| JsSubExistingQueueDoesNotMatchRequestedQueue | SUB   | 90015 | Existing consumer deliver group does not match requested queue / deliver group.                     |
| JsSubExistingConsumerCannotBeModified        | SUB   | 90016 | Existing consumer cannot be modified.                                                               |
| JsSubConsumerNotFoundRequiredInBind          | SUB   | 90017 | Consumer not found, required in bind mode.                                                          |
| JsSubOrderedNotAllowOnQueues                 | SUB   | 90018 | Ordered consumer not allowed on queues.                                                             |
| JsSubPushCantHaveMaxBatch                    | SUB   | 90019 | Push subscriptions cannot supply max batch.                                                         |
| JsSubPushCantHaveMaxBytes                    | SUB   | 90020 | Push subscriptions cannot supply max bytes.                                                         |
| JsConsumerCreate290NotAvailable              | CON   | 90301 | Name field not valid when v2.9.0 consumer create api is not available.                              |
| JsConsumerNameDurableMismatch                | CON   | 90302 | Name must match durable if both are supplied.                                                       |
| OsObjectNotFound                             | OS    | 90201 | The object was not found.                                                                           |
| OsObjectIsDeleted                            | OS    | 90202 | The object is deleted.                                                                              |
| OsObjectAlreadyExists                        | OS    | 90203 | An object with that name already exists.                                                            |
| OsCantLinkToLink                             | OS    | 90204 | A link cannot link to another link.                                                                 |
| OsGetDigestMismatch                          | OS    | 90205 | Digest does not match meta data.                                                                    |
| OsGetChunksMismatch                          | OS    | 90206 | Number of chunks does not match meta data.                                                          |
| OsGetSizeMismatch                            | OS    | 90207 | Total size does not match meta data.                                                                |
| OsGetLinkToBucket                            | OS    | 90208 | Cannot get object, it is a link to a bucket.                                                        |
| OsLinkNotAllowOnPut                          | OS    | 90209 | Link not allowed in metadata when putting an object.                                                |

### Message Acknowledgements

There are multiple types of acknowledgements in JetStream:

* `Msg.Ack()`: Acknowledges a message.
* `Msg.AckSync(timeout)`: Acknowledges a message and waits for a confirmation. When used with deduplications this creates exactly once delivery guarantees (within the deduplication window).  This may significantly impact performance of the system.
* `Msg.Nak()`: A negative acknowledgment indicating processing failed and the message should be resent later.
* `Msg.Term()`: Never send this message again, regardless of configuration.
* `Msg.InProgress()`:  The message is being processed and reset the redelivery timer in the server.  The message must be acknowledged later when processing is complete.

Note that exactly once delivery guarantee can be achieved by using a consumer with explicit ack mode attached to stream setup with a deduplication window and using the `ackSync` to acknowledge messages.  The guarantee is only valid for the duration of the deduplication window.

## Benchmarking the NATS .NET Client

Benchmarking the NATS .NET Client is simple - run the benchmark application with a default NATS server running.  Tests can be customized, run benchmark -h for more details.  In order to get the best out of your test, update the priority of the benchmark application and the NATS server:

```
start /B /REALTIME nats-server.exe
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

* [x] Key Value
* [x] Ordered Consumer
* [x] Object Store
* [x] JetStream
* [ ] Another performance pass - look at stream directly over socket, contention, fastpath optimizations, rw locks.
* [ ] Rx API (unified over NATS Streaming?)
* [ ] Allow configuration for performance tuning (buffer sizes), defaults based on plaform.
* [ ] Azure Service Bus Connector
* [ ] Visual Studio [Starter Kit](https://msdn.microsoft.com/en-us/library/ccd9ychb.aspx)
* [x] Expand Unit Tests to test internals (namely Parsing)
* [X] Travis CI (Used AppVeyor instead)
* [X] [.NET Core](https://github.com/dotnet/core) compatibility, TLS required.
* [X] Convert unit tests to xunit
* [X] Comprehensive benchmarking
* [X] TLS
* [X] Encoding (Serialization/Deserialization)
* [X] Update delegates from traditional model to custom
* [X] NuGet package
* [X] Strong name the assembly

Any suggestions and/or contributions are welcome!
