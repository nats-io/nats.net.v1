
@setlocal
@echo off

REM Your version of csc.exe must be in the path.
set PATH=C:\Windows\Microsoft.NET\Framework64\v4.0.30319;%PATH%

mkdir bin 2>NUL

cd NATS
csc.exe /nologo /out:..\bin\NATS.Client.DLL /target:library /doc:NATS.XML /optimize+ *.cs Properties\*.cs

cd ..\examples\publish
csc.exe /nologo /out:..\..\bin\Publish.exe /r:..\..\bin\NATS.Client.dll /target:exe /optimize+ *.cs Properties\*.cs

cd ..\subscribe
csc.exe /nologo /out:..\..\bin\Subscribe.exe /r:..\..\bin\NATS.Client.dll /target:exe /optimize+ *.cs Properties\*.cs

cd ..\queuegroup
csc.exe /nologo /out:..\..\bin\Queuegroup.exe /r:..\..\bin\NATS.Client.dll /target:exe /optimize+ *.cs Properties\*.cs

cd ..\requestor
csc.exe /nologo /out:..\..\bin\Requestor.exe /r:..\..\bin\NATS.Client.dll /target:exe /optimize+ *.cs Properties\*.cs

cd ..\replier
csc.exe /nologo /out:..\..\bin\replier.exe /r:..\..\bin\NATS.Client.dll /target:exe /optimize+ *.cs Properties\*.cs

cd ..\..\



