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
            tableSql.ShouldBe(@"CREATE TABLE IF NOT EXISTS [SampleModel] (
    [Id] INTEGER PRIMARY KEY NOT NULL,
    [Text] TEXT NOT NULL,
    [Int] INTEGER NOT NULL,
    [Decimal] REAL NOT NULL,
    [Double] REAL NOT NULL,
    [Float] REAL NOT NULL,
    [Flag] INTEGER NOT NULL,
    [Binary] BLOB NOT NULL,
    [Key] TEXT NOT NULL,
    [DateTime] TEXT NOT NULL,
    [DateTimeOffset] TEXT NOT NULL
);");
        }

        [Test]
        public void When_generating_full_text_search_table_sql()
        {
            Should.Throw<KeyNotFoundException>(() => SqliteSqlGenerator.FtsTable<SampleModel>(m => m.Composite))
                .Message.ShouldBe("Could not find a mapping for property: Composite. Ensure it is not marked with an IgnoreAttribute.");

            var ftsTableSql = SqliteSqlGenerator.FtsTable<SampleModel>(m => m.Flag, m => m.Text, m => m.Guid);
            ftsTableSql.ShouldNotBeNullOrWhiteSpace();
            ftsTableSql.ShouldBe(@"CREATE VIRTUAL TABLE IF NOT EXISTS SampleModel_fts USING FTS4 (content='SampleModel', [Flag], [Text], [Key]);

CREATE TRIGGER IF NOT EXISTS SampleModel_bu BEFORE UPDATE ON SampleModel BEGIN
    DELETE FROM SampleModel_fts WHERE docId = old.rowId;
END;

CREATE TRIGGER IF NOT EXISTS SampleModel_bd BEFORE DELETE ON SampleModel BEGIN
    DELETE FROM SampleModel_fts WHERE docId = old.rowId;
END;

CREATE TRIGGER IF NOT EXISTS SampleModel_au AFTER UPDATE ON SampleModel BEGIN
    INSERT INTO SampleModel_fts (docId, [Flag], [Text], [Key]) VALUES (new.rowId, new.[Flag], new.[Text], new.[Key]);
END;

CREATE TRIGGER IF NOT EXISTS SampleModel_ai AFTER INSERT ON SampleModel BEGIN
    INSERT INTO SampleModel_fts (docId, [Flag], [Text], [Key]) VALUES (new.rowId, new.[Flag], new.[Text], new.[Key]);
END;");
        }
    }
}