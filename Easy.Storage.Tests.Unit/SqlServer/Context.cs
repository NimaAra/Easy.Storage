namespace Easy.Storage.Tests.Unit.SqlServer
{
    using System;
    using System.IO;
    using System.Reflection;

    internal class Context
    {
        protected static string ConnectionString
        {
            get
            {
                var assembly = Assembly.GetExecutingAssembly();
                var uri = new Uri(assembly.CodeBase);
                return File.ReadAllText(Path.Combine(Path.GetDirectoryName(uri.LocalPath), "SqlServer\\connection.string"));
            }
        }

        protected static string TableQuery => "IF OBJECT_ID('Person', 'U') IS NULL" 
                                              + " CREATE TABLE Person (" 
                                              + " Id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,"
                                              + " Name NVARCHAR(50) NULL," 
                                              + " Age INTEGER NOT NULL);";

        protected static string ViewQuery => "IF OBJECT_ID('Person_view', 'V') IS NULL\r\n"
                                              + "EXEC('CREATE VIEW Person_view AS SELECT * FROM Person')";

        protected static bool IsRunningLocaly
        {
            get
            {
                var assembly = Assembly.GetExecutingAssembly();
                var uri = new Uri(assembly.CodeBase);
                var file = new FileInfo(Path.Combine(Path.GetDirectoryName(uri.LocalPath), "SqlServer\\connection.string"));
                return file.Exists;
            }
        }
    }
}