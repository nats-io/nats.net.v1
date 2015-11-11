@setlocal
@echo off

REM This builds the NATS .NET Client NuGet package, and is 
REM intended for use by Apcera to create NuGet Packages.

nuget >nul 2>&1
if errorlevel 9009 if not errorlevel 9010 (
    echo 'nuget.exe' is not in the path.
    goto End
)

if NOT EXIST ..\bin\NATS.Client.DLL (
    echo Cannot find ..\bin\NATS.Client.DLL
    goto End
)


if NOT EXIST ..\bin\NATS.Client.XML (
    echo Cannot find ..\bin\NATS.Client.DLL
    goto End
)

mkdir tmp 2>NUL
mkdir tmp\lib 2>NUL
mkdir tmp\lib\net45 2>NUL

copy ..\bin\NATS.Client.DLL tmp\lib\net45 1>NUL
copy ..\bin\NATS.Client.XML tmp\lib\net45 1>NUL


REM (to recreate) nuget spec -f -Verbosity detailed -AssemblyPath NATS.Client.DLL

cd tmp

copy ..\NATS.Client.nuspec . 1>NUL

nuget pack NATS.Client.nuspec

move *.nupkg .. 1>NUL

cd ..

:End

rmdir /S /Q tmp 2>NUL
