dotnet restore NATS.Client
dotnet build -c Release NATS.Client
dotnet pack -c Release NATS.Client

dotnet restore examples NATSUnitTests
dotnet build -c Release examples/Benchmark
dotnet build -c Release examples/Publish
dotnet build -c Release examples/Subscribe
dotnet build -c Release examples/Requestor
dotnet build -c Release examples/Replier

msbuild NATScore.sln /logger:"C:\Program Files\AppVeyor\BuildAgent\Appveyor.MSBuildLogger.dll"
