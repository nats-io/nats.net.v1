
@setlocal
@echo off

REM Your version of csc.exe must be in the path.
set PATH=C:\Windows\Microsoft.NET\Framework64\v4.0.30319;%PATH%

mkdir bin 2>NUL
mkdir bin\net45 2>NUL

set OUTPUTDIR=..\bin\net45

cd NATS.Client
csc.exe /nologo /define:NET45 /out:%OUTPUTDIR%\NATS.Client.DLL /target:library /doc:%OUTPUTDIR%\NATS.Client.XML /optimize+ *.cs Properties\*.cs

cd ..\examples\publish
csc.exe /nologo /out:..\%OUTPUTDIR%\Publish.exe /r:..\%OUTPUTDIR%\NATS.Client.dll /target:exe /optimize+ *.cs Properties\*.cs

cd ..\subscribe
csc.exe /nologo /out:..\%OUTPUTDIR%\Subscribe.exe /r:..\%OUTPUTDIR%\NATS.Client.dll /target:exe /optimize+ *.cs Properties\*.cs

cd ..\queuegroup
csc.exe /nologo /out:..\%OUTPUTDIR%\Queuegroup.exe /r:..\%OUTPUTDIR%\NATS.Client.dll /target:exe /optimize+ *.cs Properties\*.cs

cd ..\requestor
csc.exe /nologo /out:..\%OUTPUTDIR%\Requestor.exe /r:..\%OUTPUTDIR%\NATS.Client.dll /target:exe /optimize+ *.cs Properties\*.cs

cd ..\replier
csc.exe /nologo /out:..\%OUTPUTDIR%\Replier.exe /r:..\%OUTPUTDIR%\NATS.Client.dll /target:exe /optimize+ *.cs Properties\*.cs

cd ..\benchmark
csc.exe /nologo /out:..\%OUTPUTDIR%\Benchmark.exe /r:..\%OUTPUTDIR%\NATS.Client.dll /target:exe /optimize+ *.cs Properties\*.cs

cd ..\..\



