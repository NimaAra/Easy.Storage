namespace Easy.Storage.Tests.Unit.Filter
{
    using Easy.Storage.Common;
    using Easy.Storage.Common.Filter;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class FilterTests
    {
        private static readonly Table _table = 
            Table.MakeOrGet<Person>(GenericSQLDialect.Instance, string.Empty);

        [Test]
        public void When_creating_a_simple_equality_filter()
        {
            var filter = Query<Person>.Make(_table).Filter
                .And(p => p.Id, Operator.Equal, 1);

            filter.Parameters.ShouldNotBeNull();
            filter.Parameters["Id1"].ShouldBe(1);

            var sql = filter.GetSQL();
            sql.ShouldBe(@"SELECT
    [Person].[Id] AS 'Id',
    [Person].[Name] AS 'Name',
    [Person].[Age] AS 'Age'
FROM [Person]
WHERE
    1 = 1
AND
    ([Id]=@Id1);");
        }

        [Test]
        public void When_creating_a_simple_inequality_filter()
        {
            var filter = Query<Person>.Make(_table).Filter
                .And(p => p.Name, Operator.NotEqual, "Bar");

            filter.Parameters.ShouldNotBeNull();
            filter.Parameters["Name1"].ShouldBe("Bar");

            var sql = filter.GetSQL();
            sql.ShouldBe(@"SELECT
    [Person].[Id] AS 'Id',
    [Person].[Name] AS 'Name',
    [Person].[Age] AS 'Age'
FROM [Person]
WHERE
    1 = 1
AND
    ([Name]<>@Name1);");
        }

        [Test]
        public void When_creating_an_in_filter_as_and()
        {
            var filter = Query<Person>.Make(_table).Filter
                .AndIn(p => p.Id, new long[] { 1, 2, 3 });

            filter.Parameters.ShouldNotBeNull();
            filter.Parameters["Id1"].ShouldBe(new[] { 1, 2, 3 });

            var sql = filter.GetSQL();
            sql.ShouldBe(@"SELECT
    [Person].[Id] AS 'Id',
    [Person].[Name] AS 'Name',
    [Person].[Age] AS 'Age'
FROM [Person]
WHERE
    1 = 1
AND
    ([Id] IN @Id1);");
        }

        [Test]
        public void When_creating_a_notin_filter_as_and()
        {
            var filter = Query<Person>.Make(_table).Filter
                .AndNotIn(p => p.Id, new long[] { 1, 2, 3 });

            filter.Parameters.ShouldNotBeNull();
            filter.Parameters["Id1"].ShouldBe(new[] { 1, 2, 3 });

            var sql = filter.GetSQL();
            sql.ShouldBe(@"SELECT
    [Person].[Id] AS 'Id',
    [Person].[Name] AS 'Name',
    [Person].[Age] AS 'Age'
FROM [Person]
WHERE
    1 = 1
AND
    ([Id] NOT IN @Id1);");
        }

        [Test]
        public void When_creating_an_and_filter()
        {
            var filter = Query<Person>.Make(_table).Filter
                .And(p => p.Id, Operator.Equal, 1)
                .And(p => p.Name, Operator.Equal, "Foo");

            filter.Parameters.ShouldNotBeNull();
            filter.Parameters["Id1"].ShouldBe(1);
            filter.Parameters["Name2"].ShouldBe("Foo");

            var sql = filter.GetSQL();
            sql.ShouldBe(@"SELECT
    [Person].[Id] AS 'Id',
    [Person].[Name] AS 'Name',
    [Person].[Age] AS 'Age'
FROM [Person]
WHERE
    1 = 1
AND
    ([Id]=@Id1)
AND
    ([Name]=@Name2);");
        }

        [Test]
        public void When_creating_an_or_filter()
        {
            var filter = Query<Person>.Make(_table).Filter
                .And(p => p.Id, Operator.Equal, 1)
                .Or(p => p.Name, Operator.Equal, "Foo");

            filter.Parameters.ShouldNotBeNull();
            filter.Parameters["Id1"].ShouldBe(1);
            filter.Parameters["Name2"].ShouldBe("Foo");

            var sql = filter.GetSQL();
            sql.ShouldBe(@"SELECT
    [Person].[Id] AS 'Id',
    [Person].[Name] AS 'Name',
    [Person].[Age] AS 'Age'
FROM [Person]
WHERE
    1 = 1
AND
    ([Id]=@Id1)
OR
    ([Name]=@Name2);");
        }

        [Test]
        public void When_creating_an_in_filter_as_or()
        {
            var filter = Query<Person>.Make(_table).Filter
                .And(p => p.Id, Operator.Equal, 1)
                .OrIn(p => p.Name, new[] { "Foo", "Bar" });

            filter.Parameters.ShouldNotBeNull();
            filter.Parameters["Id1"].ShouldBe(1);
            filter.Parameters["Name2"].ShouldBe(new[] { "Foo", "Bar" });

            var sql = filter.GetSQL();
            sql.ShouldBe(@"SELECT
    [Person].[Id] AS 'Id',
    [Person].[Name] AS 'Name',
    [Person].[Age] AS 'Age'
FROM [Person]
WHERE
    1 = 1
AND
    ([Id]=@Id1)
OR
    ([Name] IN @Name2);");
        }

        [Test]
        public void When_creating_a_notin_filter_as_or()
        {
            var filter = Query<Person>.Make(_table).Filter
                .And(p => p.Id, Operator.Equal, 1)
                .OrNotIn(p => p.Name, new[] { "Foo", "Bar" });

            filter.Parameters.ShouldNotBeNull();
            filter.Parameters["Id1"].ShouldBe(1);
            filter.Parameters["Name2"].ShouldBe(new[] { "Foo", "Bar" });

            var sql = filter.GetSQL();
            sql.ShouldBe(@"SELECT
    [Person].[Id] AS 'Id',
    [Person].[Name] AS 'Name',
    [Person].[Age] AS 'Age'
FROM [Person]
WHERE
    1 = 1
AND
    ([Id]=@Id1)
OR
    ([Name] NOT IN @Name2);");
        }
    }
}