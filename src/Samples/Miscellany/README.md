# Client Compatibility Test

```text
Usage: Miscellany <program> <args...>

Available programs:
    PubSub
    RequestReply
    KvIntro
    ScatterGather
    ServiceCrossClientValidator
    SimplificationMigration
    ClientCompatibility
```

## Pre-requisites

1. .NET net46
2. dotbuild

## How to build and run the test.

From the root folder....

Change directory to Miscellany

```shell
> cd src\Samples\Miscellany>
```

Build the program
```shell
> dotnet build -c Release
```

## ClientCompatibility

Start the responder...
```shell
> bin\Release\net46\Miscellany.exe ClientCompatibility
```

You will see this when the engine is ready... 
```shell
[4:31:52 PM] Url: nats://localhost:4222
[4:31:52 PM] Ready
```

You can start the client compatibility CLI, for instance:
```shell
client-compatibility suite object-store
```

## Server Url

You can supply the url for the server in two ways. If not supplied, the program will assume `nats://localhost:4222`

1. The program first checks the command line
```shell
> bin\Release\net46\Miscellany.exe ClientCompatibility nats://myhost:4333
```

2. If there is no url on the command line, it checks the NATS_URL environment variable.

3. If it's not provided in arguments or environment, the default is used.