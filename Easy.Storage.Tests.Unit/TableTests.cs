namespace Easy.Storage.Tests.Unit
{
    using System;
    using System.Collections.Generic;
    using Easy.Storage.Common;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class TableTests
    {
        [Test]
        public void When_creating_table_for_model_with_no_id_or_primary_key_attribute()
        {
            Should.Throw<InvalidOperationException>(() => Table.Get<ModelWithNoIdOrPrimaryKey>())
                .Message.ShouldBe("The model does not have a default 'Id' property specified or any it's members marked as primary key.");
        }

        [Test]
        public void When_creating_a_table_for_model_with_a_default_id_property()
        {
            var table = Table.Get<SampleModel>();

            table.ShouldNotBeNull();
            table.Name.ShouldBe("[SampleModel]");
            table.GetColumnName("Text").ShouldBe("[Text]");
            table.GetColumnName("Guid").ShouldBe("[Key]");
            Should.Throw<KeyNotFoundException>(() => table.GetColumnName("Composite").ShouldBe("[Text]"))
                .Message.ShouldBe("The given key was not present in the dictionary.");

            var sqlQuery = table.GetSqlWithClause<SampleModel, string>(m => m.Text, "SELECT * FROM SampleModel;", true);
            sqlQuery.ShouldBe("SELECT * FROM SampleModel\r\nAND\r\n    ([Text]  = @Value);");

            table.Select.ShouldBe("SELECT\r\n"
                    + "    [Id] AS Id,\r\n"
                    + "    [Text] AS Text,\r\n"
                    + "    [Int] AS Int,\r\n"
                    + "    [Decimal] AS Decimal,\r\n"
                    + "    [Double] AS Double,\r\n"
                    + "    [Float] AS Float,\r\n"
                    + "    [Flag] AS Flag,\r\n"
                    + "    [Binary] AS Binary,\r\n"
                    + "    [Key] AS Guid,\r\n"
                    + "    [DateTime] AS DateTime,\r\n"
                    + "    [DateTimeOffset] AS DateTimeOffset\r\n"
                    + "FROM [SampleModel]\r\nWHERE\r\n    1 = 1;");

            table.Insert.ShouldBe("INSERT INTO [SampleModel]\r\n"
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

            table.UpdateDefault.ShouldBe("UPDATE [SampleModel] SET\r\n"
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

            table.UpdateCustom.ShouldBe("UPDATE [SampleModel] SET\r\n"
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