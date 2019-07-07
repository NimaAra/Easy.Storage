@echo off

set releaseVersion=%1

dotnet restore .\Easy.Storage.SQLServer
dotnet pack .\Easy.Storage.SQLServer\Easy.Storage.SQLServer.csproj --output .\nupkgs --configuration Release /p:Version=%releaseVersion% --include-symbols --include-source