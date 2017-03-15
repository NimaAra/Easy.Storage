namespace Easy.Storage.Tests.Unit.SQLServer
{
    using System;
    using System.IO;
    using Easy.Storage.Common.Attributes;

    internal class Context
    {
        private static readonly FileInfo ConnectionStringsFile = new FileInfo("C:\\connection.string");

        protected static string ConnectionString => File.ReadAllText(ConnectionStringsFile.FullName);

        protected static string DefaultTableQuery => "IF OBJECT_ID('Person', 'U') IS NULL" 
                                              + " CREATE TABLE Person (" 
                                              + " Id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,"
                                              + " Name NVARCHAR(50) NOT NULL," 
                                              + " Age INTEGER NOT NULL);";

        protected static string TableWithStringIdQuery => @"IF OBJECT_ID('TableWithStringId', 'U') IS NULL
CREATE TABLE [dbo].[TableWithStringId] (
	[Id] NVARCHAR(100) NOT NULL,
	[Name] NVARCHAR(50) NOT NULL,
	[Age] INTEGER NOT NULL,

	CONSTRAINT [PK_TableWithStringId] PRIMARY KEY CLUSTERED
	(
		[Id] ASC,
		[Name] ASC
	) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY];";

        protected static string TableWithGuidIdQuery => @"IF OBJECT_ID('TableWithGuidId', 'U') IS NULL
CREATE TABLE [dbo].[TableWithGuidId] (
	[Id] UNIQUEIDENTIFIER PRIMARY KEY,
	[Name] NVARCHAR(50) NOT NULL,
	[Age] INTEGER NOT NULL	
) ON [PRIMARY];";

        protected static string ViewQuery => "IF OBJECT_ID('Person_view', 'V') IS NULL\r\n"
                                              + "EXEC('CREATE VIEW Person_view AS SELECT * FROM Person')";

        protected static bool IsRunningLocaly => ConnectionStringsFile.Exists;
    }

    [Alias("[dbo].[TableWithStringId]")]
    internal sealed class TableWithStringIdQuery
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
    }

    [Alias("[dbo].[TableWithGuidId]")]
    internal sealed class TableWithGuidIdQuery
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
    }
}