@setlocal
@echo off

REM Requirements:  Nuget and Visual Studio must be installed and setup
REM in the environment.
REM e.g. for Visual Studio community 2015, run "%VS140COMNTOOLS%\vsvars32.bat"

nuget restore NATSnet45.sln
msbuild NATSnet45.sln /nologo /verbosity:minimal /t:Rebuild /p:Configuration=Release
