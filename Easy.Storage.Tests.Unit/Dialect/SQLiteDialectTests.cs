namespace Easy.Storage.Tests.Unit.Dialect
{
    using System.IO;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Filter;
    using Easy.Storage.SQLite;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class SQLiteDialectTests
    {
        [Test]
        public void When_creating_a_sqlite_dialect()
        {
            var instanceOne = SQLiteDialect.Instance;
            var instanceTwo = SQLiteDialect.Instance;

            instanceOne.ShouldBe(instanceTwo);
            instanceOne.ShouldBeSameAs(instanceTwo);

            SQLiteDialect.Instance.Type.ShouldBe(DialectType.SQLite);
        }

        [Test]
        public void When_getting_partial_update_query()
        {
            var dialect = SQLiteDialect.Instance;
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
            var dialect = SQLiteDialect.Instance;
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
);
SELECT last_insert_rowid() AS Id;");

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
);
SELECT last_insert_rowid() AS Id;");

            var lonelyTable = Table.MakeOrGet<Lonely>(dialect);
            dialect.GetInsertQuery(lonelyTable, true)
                .ShouldBe(@"INSERT INTO [Lonely]
(
    [Id]
)
VALUES
(
    @Id
);
SELECT last_insert_rowid() AS Id;");

            dialect.GetInsertQuery(lonelyTable, false)
                .ShouldBe(@"INSERT INTO [Lonely]
(
    
)
VALUES
(
    
);
SELECT last_insert_rowid() AS Id;");
        }

        [Test]
        public void When_getting_partial_insert_query()
        {
            var dialect = SQLiteDialect.Instance;
            var table = Table.MakeOrGet<Person>(dialect);

            var itemWithAllProperties = new { Id = 1, Name = "Foo", Age = 123 };
            dialect.GetPartialInsertQuery<Person>(table, itemWithAllProperties)
                .ShouldBe(@"INSERT INTO [Person]
(
    [Id],
    [Name],
    [Age]
) VALUES (
    @Id,
    @Name,
    @Age
);
SELECT last_insert_rowid() AS Id;");

            var itemWithSomeProperties = new { Name = "Foo" };
            dialect.GetPartialInsertQuery<Person>(table, itemWithSomeProperties)
                .ShouldBe(@"INSERT INTO [Person]
(
    [Name]
) VALUES (
    @Name
);
SELECT last_insert_rowid() AS Id;");

            var itemWithNoProperties = new { };
            Should.Throw<InvalidDataException>(() => dialect.GetPartialInsertQuery<Person>(table, itemWithNoProperties))
                .Message.ShouldBe("Unable to find any properties in: item");

            var lonelyTable = Table.MakeOrGet<Lonely>(dialect);
            var lonely = new Lonely { Id = 1 };
            dialect.GetPartialInsertQuery<Lonely>(lonelyTable, lonely)
                .ShouldBe(@"INSERT INTO [Lonely]
(
    [Id]
) VALUES (
    @Id
);
SELECT last_insert_rowid() AS Id;");
        }
    }
}