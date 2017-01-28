namespace Easy.Storage.Tests.Unit.SQLite
{
    using System.Collections.Generic;
    using Easy.Storage.SQLite;
    using Easy.Storage.SQLite.Models;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class SQLiteSQLGeneratorTests
    {
        [Test]
        public void When_generating_table_sql()
        {
            var tableSql = SQLiteSQLGenerator.Table<SampleModel>();

            tableSql.ShouldNotBeNullOrWhiteSpace();
            tableSql.ShouldBe("CREATE TABLE IF NOT EXISTS SampleModel (\r\n"
                        + "    [_Entry_TimeStamp_Epoch_ms_] INTEGER DEFAULT (CAST((julianday('now') - 2440587.5)*86400000 AS INTEGER)),\r\n"
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
        public void When_generating_full_text_search_with_content_table_sql()
        {
            Should.Throw<KeyNotFoundException>(() => SQLiteSQLGenerator.FTSTable<SampleModel>(FTSTableType.Content, m => m.Composite))
                .Message.ShouldBe("Could not find a mapping for property: Composite. Ensure it is not marked with an IgnoreAttribute.");

            var ftsTableSql = SQLiteSQLGenerator.FTSTable<SampleModel>(FTSTableType.Content, m => m.Flag, m => m.Text, m => m.Guid);
            ftsTableSql.ShouldNotBeNullOrWhiteSpace();
            ftsTableSql.ShouldBe("CREATE VIRTUAL TABLE IF NOT EXISTS SampleModel_fts USING FTS4 ([Flag], [Text], [Key]);");
        }

        [Test]
        public void When_generating_full_text_search_content_less_table_sql()
        {
            Should.Throw<KeyNotFoundException>(() => SQLiteSQLGenerator.FTSTable<SampleModel>(FTSTableType.ContentLess, m => m.Composite))
                .Message.ShouldBe("Could not find a mapping for property: Composite. Ensure it is not marked with an IgnoreAttribute.");

            var ftsTableSql = SQLiteSQLGenerator.FTSTable<SampleModel>(FTSTableType.ContentLess, m => m.Flag, m => m.Text, m => m.Guid);
            ftsTableSql.ShouldNotBeNullOrWhiteSpace();
            ftsTableSql.ShouldBe("CREATE VIRTUAL TABLE IF NOT EXISTS SampleModel_fts USING FTS4 (content=\"\", [Flag], [Text], [Key]);");
        }

        [Test]
        public void When_generating_full_text_search_external_content_table_sql()
        {
            Should.Throw<KeyNotFoundException>(() => SQLiteSQLGenerator.FTSTable<SampleModel>(FTSTableType.ExternalContent, m => m.Composite))
                .Message.ShouldBe("Could not find a mapping for property: Composite. Ensure it is not marked with an IgnoreAttribute.");

            var ftsTableSql = SQLiteSQLGenerator.FTSTable<SampleModel>(FTSTableType.ExternalContent, m => m.Flag, m => m.Text, m => m.Guid);
            ftsTableSql.ShouldNotBeNullOrWhiteSpace();
            ftsTableSql.ShouldBe("CREATE VIRTUAL TABLE IF NOT EXISTS SampleModel_fts USING FTS4 (content=\"SampleModel\", [Flag], [Text], [Key]);\r\n\r\n"
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

        [Test]
        public void When_generating_table_for_model_with_inheritance()
        {
            var parentTableSql = SQLiteSQLGenerator.Table<Parent>();

            parentTableSql.ShouldNotBeNullOrWhiteSpace();
            parentTableSql.ShouldBe("CREATE TABLE IF NOT EXISTS Parent (\r\n"
                       + "    [_Entry_TimeStamp_Epoch_ms_] INTEGER DEFAULT (CAST((julianday('now') - 2440587.5)*86400000 AS INTEGER)),\r\n"
                       + "    [Id] INTEGER PRIMARY KEY NOT NULL,\r\n"
                       + "    [Name] TEXT NOT NULL,\r\n"
                       + "    [Age] INTEGER NOT NULL\r\n);");

            var childTableSql = SQLiteSQLGenerator.Table<Child>();

            childTableSql.ShouldNotBeNullOrWhiteSpace();
            childTableSql.ShouldBe("CREATE TABLE IF NOT EXISTS Child (\r\n"
                       + "    [_Entry_TimeStamp_Epoch_ms_] INTEGER DEFAULT (CAST((julianday('now') - 2440587.5)*86400000 AS INTEGER)),\r\n"
                       + "    [Toy] TEXT NOT NULL,\r\n"
                       + "    [PetName] TEXT NOT NULL,\r\n"
                       + "    [Id] INTEGER PRIMARY KEY NOT NULL,\r\n"
                       + "    [Name] TEXT NOT NULL,\r\n"
                       + "    [Age] INTEGER NOT NULL\r\n);");
        }
    }
}