namespace Easy.Storage.Tests.Unit
{
    using System.Collections.Generic;
    using Easy.Storage.Common;
    using Easy.Storage.SQLite;
    using Easy.Storage.SQLServer;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class OverrriddenTableTests
    {
        [Test]
        public void When_creating_table()
        {
            var table = Table.MakeOrGet<Person>(GenericSQLDialect.Instance, "Foo");
            
            table.Dialect.ShouldBe(GenericSQLDialect.Instance);
            table.Dialect.Type.ShouldBe(DialectType.Generic);
            table.ModelType.ShouldBe(typeof(Person));
            table.Name.ShouldBe("[Foo]");
            table.HasIdentityColumn.ShouldBeTrue();
            table.Select.ShouldBe("SELECT\r\n"
                    + "    [Foo].[Id] AS 'Id',\r\n"
                    + "    [Foo].[Name] AS 'Name',\r\n"
                    + "    [Foo].[Age] AS 'Age'\r\n"
                    + "FROM [Foo]\r\nWHERE\r\n    1 = 1;");

            table.InsertIdentity.ShouldBe("INSERT INTO [Foo]\r\n"
                    + "(\r\n"
                    + "    [Name],\r\n"
                    + "    [Age]\r\n"
                    + ")\r\n"
                    + "VALUES\r\n"
                    + "(\r\n"
                    + "    @Name,\r\n"
                    + "    @Age\r\n"
                    + ");");

            table.UpdateIdentity.ShouldBe("UPDATE [Foo] SET\r\n"
                    + "    [Name] = @Name,\r\n"
                    + "    [Age] = @Age\r\n"
                    + "WHERE\r\n    [Id] = @Id;");

            table.UpdateAll.ShouldBe("UPDATE [Foo] SET\r\n"
                    + "    [Id] = @Id,\r\n"
                    + "    [Name] = @Name,\r\n"
                    + "    [Age] = @Age\r\n"
                    + "WHERE\r\n    1 = 1;");

            table.Delete.ShouldBe("DELETE FROM [Foo]\r\nWHERE\r\n    1 = 1;");
        }

        [Test]
        public void When_creating_a_table_for_model_with_a_default_id_property_generic_dialect()
        {
            var table = Table.MakeOrGet<SampleModel>(GenericSQLDialect.Instance, "Foo");

            table.ShouldNotBeNull();
            table.Dialect.ShouldBe(GenericSQLDialect.Instance);
            table.Dialect.Type.ShouldBe(DialectType.Generic);
            table.ModelType.ShouldBe(typeof(SampleModel));
            table.Name.ShouldBe("[Foo]");
            table.HasIdentityColumn.ShouldBeTrue();
            table.PropertyNamesToColumns["Text"].ShouldBe("[Text]");
            table.PropertyNamesToColumns["Guid"].ShouldBe("[Key]");
            Should.Throw<KeyNotFoundException>(() => table.PropertyNamesToColumns["Composite"].ShouldBe("[Text]"))
                .Message.ShouldBe("The given key was not present in the dictionary.");

            table.Select.ShouldBe("SELECT\r\n"
                    + "    [Foo].[Id] AS 'Id',\r\n"
                    + "    [Foo].[Text] AS 'Text',\r\n"
                    + "    [Foo].[Int] AS 'Int',\r\n"
                    + "    [Foo].[Decimal] AS 'Decimal',\r\n"
                    + "    [Foo].[Double] AS 'Double',\r\n"
                    + "    [Foo].[Float] AS 'Float',\r\n"
                    + "    [Foo].[Flag] AS 'Flag',\r\n"
                    + "    [Foo].[Binary] AS 'Binary',\r\n"
                    + "    [Foo].[Key] AS 'Guid',\r\n"
                    + "    [Foo].[DateTime] AS 'DateTime',\r\n"
                    + "    [Foo].[DateTimeOffset] AS 'DateTimeOffset'\r\n"
                    + "FROM [Foo]\r\nWHERE\r\n    1 = 1;");

            table.InsertIdentity.ShouldBe("INSERT INTO [Foo]\r\n"
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

            table.UpdateIdentity.ShouldBe("UPDATE [Foo] SET\r\n"
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

            table.UpdateAll.ShouldBe("UPDATE [Foo] SET\r\n"
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

            table.Delete.ShouldBe("DELETE FROM [Foo]\r\nWHERE\r\n    1 = 1;");
        }

        [Test]
        public void When_creating_a_table_for_model_with_a_default_id_property_sqlite_dialect()
        {
            var table = Table.MakeOrGet<SampleModel>(SQLiteDialect.Instance, "Foo");

            table.ShouldNotBeNull();
            table.Name.ShouldBe("[Foo]");
            table.HasIdentityColumn.ShouldBeTrue();
            table.PropertyNamesToColumns["Text"].ShouldBe("[Text]");
            table.PropertyNamesToColumns["Guid"].ShouldBe("[Key]");
            Should.Throw<KeyNotFoundException>(() => table.PropertyNamesToColumns["Composite"].ShouldBe("[Text]"))
                .Message.ShouldBe("The given key was not present in the dictionary.");

            table.Select.ShouldBe("SELECT\r\n"
                                  + "    [Foo].[Id] AS 'Id',\r\n"
                                  + "    [Foo].[Text] AS 'Text',\r\n"
                                  + "    [Foo].[Int] AS 'Int',\r\n"
                                  + "    [Foo].[Decimal] AS 'Decimal',\r\n"
                                  + "    [Foo].[Double] AS 'Double',\r\n"
                                  + "    [Foo].[Float] AS 'Float',\r\n"
                                  + "    [Foo].[Flag] AS 'Flag',\r\n"
                                  + "    [Foo].[Binary] AS 'Binary',\r\n"
                                  + "    [Foo].[Key] AS 'Guid',\r\n"
                                  + "    [Foo].[DateTime] AS 'DateTime',\r\n"
                                  + "    [Foo].[DateTimeOffset] AS 'DateTimeOffset'\r\n"
                                  + "FROM [Foo]\r\nWHERE\r\n    1 = 1;");

            table.InsertIdentity.ShouldBe("INSERT INTO [Foo]\r\n"
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

            table.UpdateIdentity.ShouldBe("UPDATE [Foo] SET\r\n"
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

            table.UpdateAll.ShouldBe("UPDATE [Foo] SET\r\n"
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

            table.Delete.ShouldBe("DELETE FROM [Foo]\r\nWHERE\r\n    1 = 1;");
        }

        [Test]
        public void When_creating_a_table_for_model_with_a_default_id_property_sqlserver_dialect()
        {
            var table = Table.MakeOrGet<SampleModel>(SQLServerDialect.Instance, "Foo");

            table.ShouldNotBeNull();
            table.Name.ShouldBe("[Foo]");
            table.HasIdentityColumn.ShouldBeTrue();
            table.PropertyNamesToColumns["Text"].ShouldBe("[Text]");
            table.PropertyNamesToColumns["Guid"].ShouldBe("[Key]");
            Should.Throw<KeyNotFoundException>(() => table.PropertyNamesToColumns["Composite"].ShouldBe("[Text]"))
                .Message.ShouldBe("The given key was not present in the dictionary.");

            table.Select.ShouldBe("SELECT\r\n"
                                  + "    [Foo].[Id] AS 'Id',\r\n"
                                  + "    [Foo].[Text] AS 'Text',\r\n"
                                  + "    [Foo].[Int] AS 'Int',\r\n"
                                  + "    [Foo].[Decimal] AS 'Decimal',\r\n"
                                  + "    [Foo].[Double] AS 'Double',\r\n"
                                  + "    [Foo].[Float] AS 'Float',\r\n"
                                  + "    [Foo].[Flag] AS 'Flag',\r\n"
                                  + "    [Foo].[Binary] AS 'Binary',\r\n"
                                  + "    [Foo].[Key] AS 'Guid',\r\n"
                                  + "    [Foo].[DateTime] AS 'DateTime',\r\n"
                                  + "    [Foo].[DateTimeOffset] AS 'DateTimeOffset'\r\n"
                                  + "FROM [Foo]\r\nWHERE\r\n    1 = 1;");

            table.InsertIdentity.ShouldBe("DECLARE @InsertedRows AS TABLE (Id BIGINT);\r\n"
                                          + "INSERT INTO [Foo]\r\n"
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

            table.UpdateIdentity.ShouldBe("UPDATE [Foo] SET\r\n"
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

            table.UpdateAll.ShouldBe("UPDATE [Foo] SET\r\n"
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

            table.Delete.ShouldBe("DELETE FROM [Foo]\r\nWHERE\r\n    1 = 1;");
        }
    }
}