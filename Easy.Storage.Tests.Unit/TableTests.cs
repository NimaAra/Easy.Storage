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
            sqlQuery.ShouldBe(@"SELECT * FROM SampleModel
AND
    ([Text]  = @Value);");

            table.Select.ShouldBe(@"SELECT
    [Id] AS Id,
    [Text] AS Text,
    [Int] AS Int,
    [Decimal] AS Decimal,
    [Double] AS Double,
    [Float] AS Float,
    [Flag] AS Flag,
    [Binary] AS Binary,
    [Key] AS Guid,
    [DateTime] AS DateTime,
    [DateTimeOffset] AS DateTimeOffset
FROM [SampleModel]
WHERE
    1 = 1;");

            table.Insert.ShouldBe(@"INSERT INTO [SampleModel]
(
    [Text],
    [Int],
    [Decimal],
    [Double],
    [Float],
    [Flag],
    [Binary],
    [Key],
    [DateTime],
    [DateTimeOffset]
)
VALUES
(
    @Text,
    @Int,
    @Decimal,
    @Double,
    @Float,
    @Flag,
    @Binary,
    @Guid,
    @DateTime,
    @DateTimeOffset
);");

            table.UpdateDefault.ShouldBe(@"UPDATE [SampleModel] SET
    [Text] = @Text,
    [Int] = @Int,
    [Decimal] = @Decimal,
    [Double] = @Double,
    [Float] = @Float,
    [Flag] = @Flag,
    [Binary] = @Binary,
    [Key] = @Guid,
    [DateTime] = @DateTime,
    [DateTimeOffset] = @DateTimeOffset
WHERE
    [Id] = @Id;");

            table.UpdateCustom.ShouldBe(@"UPDATE [SampleModel] SET
    [Text] = @Text,
    [Int] = @Int,
    [Decimal] = @Decimal,
    [Double] = @Double,
    [Float] = @Float,
    [Flag] = @Flag,
    [Binary] = @Binary,
    [Key] = @Guid,
    [DateTime] = @DateTime,
    [DateTimeOffset] = @DateTimeOffset
WHERE
    1 = 1;");

            table.Delete.ShouldBe(@"DELETE FROM [SampleModel]
WHERE
    1 = 1;");
        }
    }
}