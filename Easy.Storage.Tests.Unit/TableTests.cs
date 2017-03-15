namespace Easy.Storage.Tests.Unit
{
    using System;
    using System.Collections.Generic;
    using Easy.Storage.Common;
    using Easy.Storage.SQLite;
    using Easy.Storage.SQLServer;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class TableTests
    {
        [Test]
        public void When_creating_table()
        {
            var table = Table.MakeOrGet<Person>(GenericSQLDialect.Instance);
            table.Dialect.ShouldBe(GenericSQLDialect.Instance);
            table.Dialect.Type.ShouldBe(DialectType.Generic);
            table.Name.ShouldBe("[Person]");
            table.Select.ShouldBe("SELECT\r\n"
                    + "    [Person].[Id] AS 'Id',\r\n"
                    + "    [Person].[Name] AS 'Name',\r\n"
                    + "    [Person].[Age] AS 'Age'\r\n"
                    + "FROM [Person]\r\nWHERE\r\n    1 = 1;");

            table.InsertIdentity.ShouldBe("INSERT INTO [Person]\r\n"
                    + "(\r\n"
                    + "    [Name],\r\n"
                    + "    [Age]\r\n"
                    + ")\r\n"
                    + "VALUES\r\n"
                    + "(\r\n"
                    + "    @Name,\r\n"
                    + "    @Age\r\n"
                    + ");");

            table.UpdateIdentity.ShouldBe("UPDATE [Person] SET\r\n"
                    + "    [Name] = @Name,\r\n"
                    + "    [Age] = @Age\r\n"
                    + "WHERE\r\n    [Id] = @Id;");

            table.UpdateAll.ShouldBe("UPDATE [Person] SET\r\n"
                    + "    [Id] = @Id,\r\n"
                    + "    [Name] = @Name,\r\n"
                    + "    [Age] = @Age\r\n"
                    + "WHERE\r\n    1 = 1;");

            table.Delete.ShouldBe("DELETE FROM [Person]\r\nWHERE\r\n    1 = 1;");
        }

        [Test]
        public void When_creating_table_for_model_with_no_id_or_identity_attribute()
        {
            Should.Throw<InvalidOperationException>(() => Table.MakeOrGet<ModelWithNoIdOrPrimaryKey>(GenericSQLDialect.Instance))
                .Message.ShouldBe("The model does not have a default 'Id' property specified or any of its members marked as Identity.");
        }

        [Test]
        public void When_creating_a_table_for_model_with_a_default_id_property_generic_dialect()
        {
            var table = Table.MakeOrGet<SampleModel>(GenericSQLDialect.Instance);

            table.ShouldNotBeNull();
            table.Name.ShouldBe("[SampleModel]");
            table.PropertyNamesToColumns["Text"].ShouldBe("[Text]");
            table.PropertyNamesToColumns["Guid"].ShouldBe("[Key]");
            Should.Throw<KeyNotFoundException>(() => table.PropertyNamesToColumns["Composite"].ShouldBe("[Text]"))
                .Message.ShouldBe("The given key was not present in the dictionary.");

            table.Select.ShouldBe("SELECT\r\n"
                    + "    [SampleModel].[Id] AS 'Id',\r\n"
                    + "    [SampleModel].[Text] AS 'Text',\r\n"
                    + "    [SampleModel].[Int] AS 'Int',\r\n"
                    + "    [SampleModel].[Decimal] AS 'Decimal',\r\n"
                    + "    [SampleModel].[Double] AS 'Double',\r\n"
                    + "    [SampleModel].[Float] AS 'Float',\r\n"
                    + "    [SampleModel].[Flag] AS 'Flag',\r\n"
                    + "    [SampleModel].[Binary] AS 'Binary',\r\n"
                    + "    [SampleModel].[Key] AS 'Guid',\r\n"
                    + "    [SampleModel].[DateTime] AS 'DateTime',\r\n"
                    + "    [SampleModel].[DateTimeOffset] AS 'DateTimeOffset'\r\n"
                    + "FROM [SampleModel]\r\nWHERE\r\n    1 = 1;");

            table.InsertIdentity.ShouldBe("INSERT INTO [SampleModel]\r\n"
                    + "(\r\n"
                    + "    [Text],\r\n"
                    + "    [Int],\r\n"
                    + "    [Decimal],\r\n"
                    + "    [Double],\r\n"
                    + "    [Float],\r\n"
                    + "    [Flag],\r\n"
                    + "    [Binary],\r\n"
                    + "    [Key],\r\n"
                    + "    [DateTime],\r\n"
                    + "    [DateTimeOffset]\r\n"
                    + ")\r\n"
                    + "VALUES\r\n"
                    + "(\r\n"
                    + "    @Text,\r\n"
                    + "    @Int,\r\n"
                    + "    @Decimal,\r\n"
                    + "    @Double,\r\n"
                    + "    @Float,\r\n"
                    + "    @Flag,\r\n"
                    + "    @Binary,\r\n"
                    + "    @Guid,\r\n"
                    + "    @DateTime,\r\n"
                    + "    @DateTimeOffset\r\n"
                    + ");");

            table.UpdateIdentity.ShouldBe("UPDATE [SampleModel] SET\r\n"
                    + "    [Text] = @Text,\r\n"
                    + "    [Int] = @Int,\r\n"
                    + "    [Decimal] = @Decimal,\r\n"
                    + "    [Double] = @Double,\r\n"
                    + "    [Float] = @Float,\r\n"
                    + "    [Flag] = @Flag,\r\n"
                    + "    [Binary] = @Binary,\r\n"
                    + "    [Key] = @Guid,\r\n"
                    + "    [DateTime] = @DateTime,\r\n"
                    + "    [DateTimeOffset] = @DateTimeOffset\r\n"
                    + "WHERE\r\n    [Id] = @Id;");

            table.UpdateAll.ShouldBe("UPDATE [SampleModel] SET\r\n"
                    + "    [Id] = @Id,\r\n"
                    + "    [Text] = @Text,\r\n"
                    + "    [Int] = @Int,\r\n"
                    + "    [Decimal] = @Decimal,\r\n"
                    + "    [Double] = @Double,\r\n"
                    + "    [Float] = @Float,\r\n"
                    + "    [Flag] = @Flag,\r\n"
                    + "    [Binary] = @Binary,\r\n"
                    + "    [Key] = @Guid,\r\n"
                    + "    [DateTime] = @DateTime,\r\n"
                    + "    [DateTimeOffset] = @DateTimeOffset\r\n"
                    + "WHERE\r\n    1 = 1;");

            table.Delete.ShouldBe("DELETE FROM [SampleModel]\r\nWHERE\r\n    1 = 1;");
        }

        [Test]
        public void When_creating_a_table_for_model_with_a_default_id_property_sqlite_dialect()
        {
            var table = Table.MakeOrGet<SampleModel>(SQLiteDialect.Instance);

            table.ShouldNotBeNull();
            table.Name.ShouldBe("[SampleModel]");
            table.PropertyNamesToColumns["Text"].ShouldBe("[Text]");
            table.PropertyNamesToColumns["Guid"].ShouldBe("[Key]");
            Should.Throw<KeyNotFoundException>(() => table.PropertyNamesToColumns["Composite"].ShouldBe("[Text]"))
                .Message.ShouldBe("The given key was not present in the dictionary.");

            table.Select.ShouldBe("SELECT\r\n"
                                  + "    [SampleModel].[Id] AS 'Id',\r\n"
                                  + "    [SampleModel].[Text] AS 'Text',\r\n"
                                  + "    [SampleModel].[Int] AS 'Int',\r\n"
                                  + "    [SampleModel].[Decimal] AS 'Decimal',\r\n"
                                  + "    [SampleModel].[Double] AS 'Double',\r\n"
                                  + "    [SampleModel].[Float] AS 'Float',\r\n"
                                  + "    [SampleModel].[Flag] AS 'Flag',\r\n"
                                  + "    [SampleModel].[Binary] AS 'Binary',\r\n"
                                  + "    [SampleModel].[Key] AS 'Guid',\r\n"
                                  + "    [SampleModel].[DateTime] AS 'DateTime',\r\n"
                                  + "    [SampleModel].[DateTimeOffset] AS 'DateTimeOffset'\r\n"
                                  + "FROM [SampleModel]\r\nWHERE\r\n    1 = 1;");

            table.InsertIdentity.ShouldBe("INSERT INTO [SampleModel]\r\n"
                                          + "(\r\n"
                                          + "    [Text],\r\n"
                                          + "    [Int],\r\n"
                                          + "    [Decimal],\r\n"
                                          + "    [Double],\r\n"
                                          + "    [Float],\r\n"
                                          + "    [Flag],\r\n"
                                          + "    [Binary],\r\n"
                                          + "    [Key],\r\n"
                                          + "    [DateTime],\r\n"
                                          + "    [DateTimeOffset]\r\n"
                                          + ")\r\n"
                                          + "VALUES\r\n"
                                          + "(\r\n"
                                          + "    @Text,\r\n"
                                          + "    @Int,\r\n"
                                          + "    @Decimal,\r\n"
                                          + "    @Double,\r\n"
                                          + "    @Float,\r\n"
                                          + "    @Flag,\r\n"
                                          + "    @Binary,\r\n"
                                          + "    @Guid,\r\n"
                                          + "    @DateTime,\r\n"
                                          + "    @DateTimeOffset\r\n"
                                          + ");\r\n"
                                          +"SELECT last_insert_rowid() AS Id;");

            table.UpdateIdentity.ShouldBe("UPDATE [SampleModel] SET\r\n"
                                         + "    [Text] = @Text,\r\n"
                                         + "    [Int] = @Int,\r\n"
                                         + "    [Decimal] = @Decimal,\r\n"
                                         + "    [Double] = @Double,\r\n"
                                         + "    [Float] = @Float,\r\n"
                                         + "    [Flag] = @Flag,\r\n"
                                         + "    [Binary] = @Binary,\r\n"
                                         + "    [Key] = @Guid,\r\n"
                                         + "    [DateTime] = @DateTime,\r\n"
                                         + "    [DateTimeOffset] = @DateTimeOffset\r\n"
                                         + "WHERE\r\n    [Id] = @Id;");

            table.UpdateAll.ShouldBe("UPDATE [SampleModel] SET\r\n"
                                     + "    [Id] = @Id,\r\n"
                                     + "    [Text] = @Text,\r\n"
                                     + "    [Int] = @Int,\r\n"
                                     + "    [Decimal] = @Decimal,\r\n"
                                     + "    [Double] = @Double,\r\n"
                                     + "    [Float] = @Float,\r\n"
                                     + "    [Flag] = @Flag,\r\n"
                                     + "    [Binary] = @Binary,\r\n"
                                     + "    [Key] = @Guid,\r\n"
                                     + "    [DateTime] = @DateTime,\r\n"
                                     + "    [DateTimeOffset] = @DateTimeOffset\r\n"
                                     + "WHERE\r\n    1 = 1;");

            table.Delete.ShouldBe("DELETE FROM [SampleModel]\r\nWHERE\r\n    1 = 1;");
        }

        [Test]
        public void When_creating_a_table_for_model_with_a_default_id_property_sqlserver_dialect()
        {
            var table = Table.MakeOrGet<SampleModel>(SQLServerDialect.Instance);

            table.ShouldNotBeNull();
            table.Name.ShouldBe("[SampleModel]");
            table.PropertyNamesToColumns["Text"].ShouldBe("[Text]");
            table.PropertyNamesToColumns["Guid"].ShouldBe("[Key]");
            Should.Throw<KeyNotFoundException>(() => table.PropertyNamesToColumns["Composite"].ShouldBe("[Text]"))
                .Message.ShouldBe("The given key was not present in the dictionary.");

            table.Select.ShouldBe("SELECT\r\n"
                                  + "    [SampleModel].[Id] AS 'Id',\r\n"
                                  + "    [SampleModel].[Text] AS 'Text',\r\n"
                                  + "    [SampleModel].[Int] AS 'Int',\r\n"
                                  + "    [SampleModel].[Decimal] AS 'Decimal',\r\n"
                                  + "    [SampleModel].[Double] AS 'Double',\r\n"
                                  + "    [SampleModel].[Float] AS 'Float',\r\n"
                                  + "    [SampleModel].[Flag] AS 'Flag',\r\n"
                                  + "    [SampleModel].[Binary] AS 'Binary',\r\n"
                                  + "    [SampleModel].[Key] AS 'Guid',\r\n"
                                  + "    [SampleModel].[DateTime] AS 'DateTime',\r\n"
                                  + "    [SampleModel].[DateTimeOffset] AS 'DateTimeOffset'\r\n"
                                  + "FROM [SampleModel]\r\nWHERE\r\n    1 = 1;");

            table.InsertIdentity.ShouldBe("DECLARE @InsertedRows AS TABLE (Id BIGINT);\r\n"
                                          + "INSERT INTO [SampleModel]\r\n"
                                          + "(\r\n"
                                          + "    [Text],\r\n"
                                          + "    [Int],\r\n"
                                          + "    [Decimal],\r\n"
                                          + "    [Double],\r\n"
                                          + "    [Float],\r\n"
                                          + "    [Flag],\r\n"
                                          + "    [Binary],\r\n"
                                          + "    [Key],\r\n"
                                          + "    [DateTime],\r\n"
                                          + "    [DateTimeOffset]\r\n"
                                          + ") OUTPUT Inserted.[Id] INTO @InsertedRows\r\n"
                                          + "VALUES\r\n"
                                          + "(\r\n"
                                          + "    @Text,\r\n"
                                          + "    @Int,\r\n"
                                          + "    @Decimal,\r\n"
                                          + "    @Double,\r\n"
                                          + "    @Float,\r\n"
                                          + "    @Flag,\r\n"
                                          + "    @Binary,\r\n"
                                          + "    @Guid,\r\n"
                                          + "    @DateTime,\r\n"
                                          + "    @DateTimeOffset\r\n"
                                          + ");\r\n"
                                          + "SELECT Id FROM @InsertedRows;");

            table.UpdateIdentity.ShouldBe("UPDATE [SampleModel] SET\r\n"
                                         + "    [Text] = @Text,\r\n"
                                         + "    [Int] = @Int,\r\n"
                                         + "    [Decimal] = @Decimal,\r\n"
                                         + "    [Double] = @Double,\r\n"
                                         + "    [Float] = @Float,\r\n"
                                         + "    [Flag] = @Flag,\r\n"
                                         + "    [Binary] = @Binary,\r\n"
                                         + "    [Key] = @Guid,\r\n"
                                         + "    [DateTime] = @DateTime,\r\n"
                                         + "    [DateTimeOffset] = @DateTimeOffset\r\n"
                                         + "WHERE\r\n    [Id] = @Id;");

            table.UpdateAll.ShouldBe("UPDATE [SampleModel] SET\r\n"
                                     + "    [Id] = @Id,\r\n"
                                     + "    [Text] = @Text,\r\n"
                                     + "    [Int] = @Int,\r\n"
                                     + "    [Decimal] = @Decimal,\r\n"
                                     + "    [Double] = @Double,\r\n"
                                     + "    [Float] = @Float,\r\n"
                                     + "    [Flag] = @Flag,\r\n"
                                     + "    [Binary] = @Binary,\r\n"
                                     + "    [Key] = @Guid,\r\n"
                                     + "    [DateTime] = @DateTime,\r\n"
                                     + "    [DateTimeOffset] = @DateTimeOffset\r\n"
                                     + "WHERE\r\n    1 = 1;");

            table.Delete.ShouldBe("DELETE FROM [SampleModel]\r\nWHERE\r\n    1 = 1;");
        }
    }
}