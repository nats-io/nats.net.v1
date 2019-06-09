@setlocal
@echo off

REM This builds the NATS .NET Client NuGet package, and is 
REM intended for use by NATS Maintainers to create NuGet Packages.

nuget >nul 2>&1
if errorlevel 9009 if not errorlevel 9010 (
    echo 'nuget.exe' is not in the path.
    goto End
)

if NOT EXIST ..\net45\NATS.Client\bin\Release\NATS.Client.DLL (
    echo Cannot find ..\net45\NATS.Client\bin\Release\\NATS.Client.DLL
    goto End
)
dir ..\net45\NATS.Client\bin\Release\NATS.Client.DLL

if NOT EXIST ..\net45\NATS.Client\bin\Release\NATS.Client.XML (
    echo Cannot find ..\net45\NATS.Client\bin\Release\NATS.Client.XML
    goto End
)

if NOT EXIST ..\NATS.Client\bin\Release\netstandard1.6 (
    echo Cannot find .NET core build.
    goto End
)
dir ..\NATS.Client\bin\Release\netstandard1.6

mkdir tmp 2>NUL
mkdir tmp\lib 2>NUL
mkdir tmp\lib\net45 2>NUL
mkdir tmp\lib\netstandard1.6 2>NUL

copy ..\net45\NATS.Client\bin\Release\NATS.Client.DLL tmp\lib\net45 1>NUL
copy ..\net45\NATS.Client\bin\Release\NATS.Client.XML tmp\lib\net45 1>NUL

REM .NET core
copy ..\NATS.Client\bin\Release\netstandard1.6\* tmp\lib\netstandard1.6 1>NUL

REM (to recreate) nuget spec -f -Verbosity detailed -AssemblyPath NATS.Client.DLL

cd tmp

copy ..\NATS.Client.nuspec . 1>NUL

nuget pack NATS.Client.nuspec

move *.nupkg .. 1>NUL

cd ..

:End

rmdir /S /Q tmp 2>NUL
