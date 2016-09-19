namespace Easy.Storage.Tests.Unit.Sqlite
{
    using System.Collections.Generic;
    using Easy.Storage.Sqlite;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class SqliteSqlGeneratorTests
    {
        [Test]
        public void When_generating_table_sql()
        {
            var tableSql = SqliteSqlGenerator.Table<SampleModel>();

            tableSql.ShouldNotBeNullOrWhiteSpace();
            tableSql.ShouldBe("CREATE TABLE IF NOT EXISTS [SampleModel] (\r\n"
                        + "    [Id] INTEGER PRIMARY KEY NOT NULL,\r\n"
                        + "    [Text] TEXT NOT NULL,\r\n"
                        + "    [Int] INTEGER NOT NULL,\r\n"
                        + "    [Decimal] REAL NOT NULL,\r\n"
                        + "    [Double] REAL NOT NULL,\r\n"
                        + "    [Float] REAL NOT NULL,\r\n"
                        + "    [Flag] INTEGER NOT NULL,\r\n"
                        + "    [Binary] BLOB NOT NULL,\r\n"
                        + "    [Key] TEXT NOT NULL,\r\n"
                        + "    [DateTime] TEXT NOT NULL,\r\n"
                        + "    [DateTimeOffset] TEXT NOT NULL\r\n);");
        }

        [Test]
        public void When_generating_full_text_search_table_sql()
        {
            Should.Throw<KeyNotFoundException>(() => SqliteSqlGenerator.FtsTable<SampleModel>(m => m.Composite))
                .Message.ShouldBe("Could not find a mapping for property: Composite. Ensure it is not marked with an IgnoreAttribute.");

            var ftsTableSql = SqliteSqlGenerator.FtsTable<SampleModel>(m => m.Flag, m => m.Text, m => m.Guid);
            ftsTableSql.ShouldNotBeNullOrWhiteSpace();
            ftsTableSql.ShouldBe("CREATE VIRTUAL TABLE IF NOT EXISTS SampleModel_fts USING FTS4 (content='SampleModel', [Flag], [Text], [Key]);\r\n\r\n"
                        + "CREATE TRIGGER IF NOT EXISTS SampleModel_bu BEFORE UPDATE ON SampleModel BEGIN\r\n"
                        + "    DELETE FROM SampleModel_fts WHERE docId = old.rowId;\r\n"
                        + "END;\r\n\r\n"
                        + "CREATE TRIGGER IF NOT EXISTS SampleModel_bd BEFORE DELETE ON SampleModel BEGIN\r\n"
                        + "    DELETE FROM SampleModel_fts WHERE docId = old.rowId;\r\n"
                        + "END;\r\n\r\n"
                        + "CREATE TRIGGER IF NOT EXISTS SampleModel_au AFTER UPDATE ON SampleModel BEGIN\r\n"
                        + "    INSERT INTO SampleModel_fts (docId, [Flag], [Text], [Key]) VALUES (new.rowId, new.[Flag], new.[Text], new.[Key]);\r\n"
                        + "END;\r\n\r\n"
                        + "CREATE TRIGGER IF NOT EXISTS SampleModel_ai AFTER INSERT ON SampleModel BEGIN\r\n"
                        + "    INSERT INTO SampleModel_fts (docId, [Flag], [Text], [Key]) VALUES (new.rowId, new.[Flag], new.[Text], new.[Key]);\r\n"
                        + "END;");
        }
    }
}