namespace Easy.Storage.Tests.Unit.SQLite
{
    using System;
    using System.Collections.Generic;
    using Easy.Storage.Common.Attributes;
    using Easy.Storage.SQLite;
    using Easy.Storage.SQLite.Models;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;
    using Ignore = Common.Attributes.IgnoreAttribute;

    [TestFixture]
    internal sealed class SQLiteSQLGeneratorTests
    {
        [Test]
        public void When_generating_table_sql()
        {
            var tableSql = SQLiteSQLGenerator.Table<SomeModel>();

            tableSql.ShouldNotBeNullOrWhiteSpace();
            tableSql.ShouldBe("CREATE TABLE IF NOT EXISTS SomeModel (\r\n"
                        + "    [_Entry_TimeStamp_Epoch_ms_] INTEGER DEFAULT (CAST((julianday('now') - 2440587.5)*86400000 AS INTEGER)),\r\n"
                        + "    [Id] INTEGER PRIMARY KEY NOT NULL,\r\n"
                        + "    [Text] TEXT NOT NULL,\r\n"
                        + "    [Int] INTEGER,\r\n"
                        + "    [Decimal] REAL NOT NULL,\r\n"
                        + "    [Double] REAL NOT NULL,\r\n"
                        + "    [Float] REAL NOT NULL,\r\n"
                        + "    [Flag] INTEGER NOT NULL,\r\n"
                        + "    [Binary] BLOB NOT NULL,\r\n"
                        + "    [Object] TEXT NOT NULL,\r\n"
                        + "    [Key] TEXT NOT NULL,\r\n"
                        + "    [DateTime] TEXT NOT NULL,\r\n"
                        + "    [DateTimeOffset] TEXT NOT NULL\r\n);");
        }

        [Test]
        public void When_generating_table_sql_for_model_with_overridden_name()
        {
            var tableSql = SQLiteSQLGenerator.Table<SomeModel>("FooFoo");

            tableSql.ShouldNotBeNullOrWhiteSpace();
            tableSql.ShouldBe("CREATE TABLE IF NOT EXISTS FooFoo (\r\n"
                              + "    [_Entry_TimeStamp_Epoch_ms_] INTEGER DEFAULT (CAST((julianday('now') - 2440587.5)*86400000 AS INTEGER)),\r\n"
                              + "    [Id] INTEGER PRIMARY KEY NOT NULL,\r\n"
                              + "    [Text] TEXT NOT NULL,\r\n"
                              + "    [Int] INTEGER,\r\n"
                              + "    [Decimal] REAL NOT NULL,\r\n"
                              + "    [Double] REAL NOT NULL,\r\n"
                              + "    [Float] REAL NOT NULL,\r\n"
                              + "    [Flag] INTEGER NOT NULL,\r\n"
                              + "    [Binary] BLOB NOT NULL,\r\n"
                              + "    [Object] TEXT NOT NULL,\r\n"
                              + "    [Key] TEXT NOT NULL,\r\n"
                              + "    [DateTime] TEXT NOT NULL,\r\n"
                              + "    [DateTimeOffset] TEXT NOT NULL\r\n);");
        }

        [Test]
        public void When_generating_full_text_search_with_content_table_sql()
        {
            Should.Throw<KeyNotFoundException>(() => SQLiteSQLGenerator.FTSTable<SomeModel>(FTSTableType.Content, m => m.Composite))
                .Message.ShouldBe("Could not find a mapping for property: Composite. Ensure it is not marked with an IgnoreAttribute.");

            var ftsTableSql = SQLiteSQLGenerator.FTSTable<SomeModel>(FTSTableType.Content, m => m.Flag, m => m.Text, m => m.Guid);
            ftsTableSql.ShouldNotBeNullOrWhiteSpace();
            ftsTableSql.ShouldBe("CREATE VIRTUAL TABLE IF NOT EXISTS SomeModel_fts USING FTS4 ([Flag], [Text], [Key]);");
        }

        [Test]
        public void When_generating_full_text_search_content_less_table_sql()
        {
            Should.Throw<KeyNotFoundException>(() => SQLiteSQLGenerator.FTSTable<SomeModel>(FTSTableType.ContentLess, m => m.Composite))
                .Message.ShouldBe("Could not find a mapping for property: Composite. Ensure it is not marked with an IgnoreAttribute.");

            var ftsTableSql = SQLiteSQLGenerator.FTSTable<SomeModel>(FTSTableType.ContentLess, m => m.Flag, m => m.Text, m => m.Guid);
            ftsTableSql.ShouldNotBeNullOrWhiteSpace();
            ftsTableSql.ShouldBe("CREATE VIRTUAL TABLE IF NOT EXISTS SomeModel_fts USING FTS4 (content=\"\", [Flag], [Text], [Key]);");
        }

        [Test]
        public void When_generating_full_text_search_external_content_table_sql()
        {
            Should.Throw<KeyNotFoundException>(() => SQLiteSQLGenerator.FTSTable<SomeModel>(FTSTableType.ExternalContent, m => m.Composite))
                .Message.ShouldBe("Could not find a mapping for property: Composite. Ensure it is not marked with an IgnoreAttribute.");

            var ftsTableSql = SQLiteSQLGenerator.FTSTable<SomeModel>(FTSTableType.ExternalContent, m => m.Flag, m => m.Text, m => m.Guid);
            ftsTableSql.ShouldNotBeNullOrWhiteSpace();
            ftsTableSql.ShouldBe("CREATE VIRTUAL TABLE IF NOT EXISTS SomeModel_fts USING FTS4 (content=\"SomeModel\", [Flag], [Text], [Key]);\r\n\r\n"
                        + "CREATE TRIGGER IF NOT EXISTS SomeModel_bu BEFORE UPDATE ON SomeModel BEGIN\r\n"
                        + "    DELETE FROM SomeModel_fts WHERE docId = old.rowId;\r\n"
                        + "END;\r\n\r\n"
                        + "CREATE TRIGGER IF NOT EXISTS SomeModel_bd BEFORE DELETE ON SomeModel BEGIN\r\n"
                        + "    DELETE FROM SomeModel_fts WHERE docId = old.rowId;\r\n"
                        + "END;\r\n\r\n"
                        + "CREATE TRIGGER IF NOT EXISTS SomeModel_au AFTER UPDATE ON SomeModel BEGIN\r\n"
                        + "    INSERT INTO SomeModel_fts (docId, [Flag], [Text], [Key]) VALUES (new.rowId, new.[Flag], new.[Text], new.[Key]);\r\n"
                        + "END;\r\n\r\n"
                        + "CREATE TRIGGER IF NOT EXISTS SomeModel_ai AFTER INSERT ON SomeModel BEGIN\r\n"
                        + "    INSERT INTO SomeModel_fts (docId, [Flag], [Text], [Key]) VALUES (new.rowId, new.[Flag], new.[Text], new.[Key]);\r\n"
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

        private sealed class SomeModel
        {
            public long Id { get; set; }
            public string Text { get; set; }
            
            [Nullable]
            public int Int { get; set; }
            public decimal Decimal { get; set; }
            public double Double { get; set; }
            public float Float { get; set; }
            public bool Flag { get; set; }
            public byte[] Binary { get; set; }
            public object Object { get; set; }


            [Alias("Key")]
            public Guid Guid { get; set; }
            public DateTime DateTime { get; set; }
            public DateTimeOffset DateTimeOffset { get; set; }

            [Ignore]
            public IEnumerable<Person> Composite { get; set; }
        }
    }
}