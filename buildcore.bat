dotnet restore NATS.Client
dotnet build -c Release NATS.Client
dotnet pack -c Release NATS.Client

dotnet build -c Release examples/Benchmark
dotnet build -c Release examples/Publish
dotnet build -c Release examples/Subscribe
dotnet build -c Release examples/Requestor
dotnet build -c Release examples/Replier