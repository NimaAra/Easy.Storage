@echo off

set releaseVersion=%1

dotnet restore .\Easy.Storage.Common
dotnet pack .\Easy.Storage.Common\Easy.Storage.Common.csproj --output ..\nupkgs --configuration Release /p:Version=%releaseVersion% --include-symbols --include-source