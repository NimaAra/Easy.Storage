namespace Easy.Storage.Tests.Unit.SqlServer
{
    using System.IO;

    internal class Context
    {
        private static readonly FileInfo ConnectionStringsFile = new FileInfo("C:\\connection.string");

        protected static string ConnectionString => File.ReadAllText(ConnectionStringsFile.FullName);

        protected static string TableQuery => "IF OBJECT_ID('Person', 'U') IS NULL" 
                                              + " CREATE TABLE Person (" 
                                              + " Id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,"
                                              + " Name NVARCHAR(50) NULL," 
                                              + " Age INTEGER NOT NULL);";

        protected static string ViewQuery => "IF OBJECT_ID('Person_view', 'V') IS NULL\r\n"
                                              + "EXEC('CREATE VIEW Person_view AS SELECT * FROM Person')";

        protected static bool IsRunningLocaly => ConnectionStringsFile.Exists;
    }
}