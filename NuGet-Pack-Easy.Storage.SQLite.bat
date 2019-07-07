@echo off

set releaseVersion=%1

dotnet restore .\Easy.Storage.SQLite
dotnet pack .\Easy.Storage.SQLite\Easy.Storage.SQLite.csproj --output .\nupkgs --configuration Release /p:Version=%releaseVersion% --include-symbols --include-source