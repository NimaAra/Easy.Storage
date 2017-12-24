namespace Easy.Storage.Tests.Unit.Dialect
{
    using System;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Filter;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class GenericSQLDialectTests
    {
        [Test]
        public void When_creating_a_generic_dialect()
        {
            var instanceOne = GenericSQLDialect.Instance;
            var instanceTwo = GenericSQLDialect.Instance;

            instanceOne.ShouldBe(instanceTwo);
            instanceOne.ShouldBeSameAs(instanceTwo);

            GenericSQLDialect.Instance.Type.ShouldBe(DialectType.Generic);
        }

        [Test]
        public void When_getting_partial_update_query()
        {
            var dialect = GenericSQLDialect.Instance;
            var table = Table.MakeOrGet<Person>(dialect);
            var filter = Query<Person>.Filter.And(x => x.Id, Operator.Equal, 1);

            var person = new Person { Age = 10, Name = "Joe" };
            dialect.GetPartialUpdateQuery(table, person, filter)
                .ShouldBe(@"UPDATE [Person] SET
    [Id] = @Id,
    [Name] = @Name,
    [Age] = @Age
WHERE 1=1
AND
    ([Id]=@Id1);");

            var item = new { Age = 20, Name = "Bob" };
            dialect.GetPartialUpdateQuery(table, item, filter)
                .ShouldBe(@"UPDATE [Person] SET
    [Age] = @Age,
    [Name] = @Name
WHERE 1=1
AND
    ([Id]=@Id1);");

            var lonelyTable = Table.MakeOrGet<Lonely>(dialect);
            var lonelyFilter = Query<Lonely>.Filter.And(x => x.Id, Operator.Equal, 1);
            var lonely = new Lonely { Id = 1 };
            dialect.GetPartialUpdateQuery(lonelyTable, lonely, lonelyFilter)
                .ShouldBe(@"UPDATE [Lonely] SET
    [Id] = @Id
WHERE 1=1
AND
    ([Id]=@Id1);");
        }

        [Test]
        public void When_getting_insert_query()
        {
            var dialect = GenericSQLDialect.Instance;
            var table = Table.MakeOrGet<Person>(dialect);

            dialect.GetInsertQuery(table, true)
                .ShouldBe(@"INSERT INTO [Person]
(
    [Id],
    [Name],
    [Age]
)
VALUES
(
    @Id,
    @Name,
    @Age
);");

            dialect.GetInsertQuery(table, false)
                .ShouldBe(@"INSERT INTO [Person]
(
    [Name],
    [Age]
)
VALUES
(
    @Name,
    @Age
);");

            var lonelyTable = Table.MakeOrGet<Lonely>(dialect);
            dialect.GetInsertQuery(lonelyTable, true)
                .ShouldBe(@"INSERT INTO [Lonely]
(
    [Id]
)
VALUES
(
    @Id
);");

            dialect.GetInsertQuery(lonelyTable, false)
                .ShouldBe(@"INSERT INTO [Lonely]
(
    
)
VALUES
(
    
);");
        }

        [Test]
        public void When_getting_partial_insert_query()
        {
            var dialect = GenericSQLDialect.Instance;
            var table = Table.MakeOrGet<Person>(dialect);

            var itemWithAllProperties = new {Id = 1, Name = "Foo", Age = 123};
            Should.Throw<NotImplementedException>(() => dialect.GetPartialInsertQuery<Person>(table, itemWithAllProperties))
                .Message.ShouldBe("The method or operation is not implemented.");
        }
    }
}