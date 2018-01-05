dotnet restore
dotnet build -c Release NATS.Client
dotnet pack -c Release NATS.Client

dotnet build -c Release examples/Benchmark -o ../../bin/netcore -f netstandard1.6
dotnet build -c Release examples/Benchmark -o ../../bin/net45 -f net45

dotnet build -c Release examples/Publish -o ../../bin/netcore -f netstandard1.6
dotnet build -c Release examples/Publish -o ../../bin/net45 -f net45

dotnet build -c Release examples/Subscribe -o ../../bin/netcore -f netstandard1.6
dotnet build -c Release examples/Subscribe -o ../../bin/net45 -f net45

dotnet build -c Release examples/Requestor -o ../../bin/netcore -f netstandard1.6
dotnet build -c Release examples/Requestor -o ../../bin/net45 -f net45

dotnet build -c Release examples/Replier -o ../../bin/netcore -f netstandard1.6
dotnet build -c Release examples/Replier -o ../../bin/net45 -f net45