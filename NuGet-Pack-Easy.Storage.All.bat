@echo off

set releaseVersion=%1

call .\NuGet-Pack-Easy.Storage.Common %releaseVersion%
call .\NuGet-Pack-Easy.Storage.SQLite %releaseVersion%
call .\NuGet-Pack-Easy.Storage.SQLServer %releaseVersion%