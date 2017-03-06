namespace Easy.Storage.Tests.Unit.Filter
{
    using Easy.Storage.Common;
    using Easy.Storage.Common.Filter;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class FilterdQueryTests
    {
        [Test]
        public void When_creating_a_filtered_query()
        {
            var queryOne = FilteredQuery.Make<Person>();
            queryOne.ShouldNotBeNull();
            queryOne.Parameters.ShouldBeEmpty();

            var sql = queryOne.Compile();
            sql.ShouldBe(@"SELECT
    [Person].[Id] AS 'Id',
    [Person].[Name] AS 'Name',
    [Person].[Age] AS 'Age'
FROM [Person]
WHERE
    1 = 1;");
        }

        [Test]
        public void When_adding_equality_clause()
        {
            var query = FilteredQuery.Make<Person>();
            query.AddClause<Person, long>(p => p.Id, Operator.Equal, 1, Formatter.AndClauseSeparator);
            query.Parameters.Count.ShouldBe(1);
            query.Parameters["Id1"].ShouldBe(1);

            var sql = query.Compile();
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
        public void When_adding_in_clause()
        {
            var query = FilteredQuery.Make<Person>();
            query.AddInClause<Person, string>(p => p.Name, Formatter.AndClauseSeparator, true, new [] { "Foo", "Bar" });
            query.Parameters.Count.ShouldBe(1);
            query.Parameters["Name1"].ShouldBe(new [] { "Foo", "Bar"});

            var sql = query.Compile();
            sql.ShouldBe(@"SELECT
    [Person].[Id] AS 'Id',
    [Person].[Name] AS 'Name',
    [Person].[Age] AS 'Age'
FROM [Person]
WHERE
    1 = 1
AND
    ([Name] IN @Name1);");
        }

        [Test]
        public void When_adding_not_in_clause()
        {
            var query = FilteredQuery.Make<Person>();
            query.AddInClause<Person, string>(p => p.Name, Formatter.AndClauseSeparator, false, new[] { "Foo", "Bar" });
            query.Parameters.Count.ShouldBe(1);
            query.Parameters["Name1"].ShouldBe(new[] { "Foo", "Bar" });

            var sql = query.Compile();
            sql.ShouldBe(@"SELECT
    [Person].[Id] AS 'Id',
    [Person].[Name] AS 'Name',
    [Person].[Age] AS 'Age'
FROM [Person]
WHERE
    1 = 1
AND
    ([Name] NOT IN @Name1);");
        }

        [Test]
        public void When_adding_filter_to_a_delete_statement()
        {
            var query = FilteredQuery.Make<Person>();
            query.AddClause<Person, string>(p => p.Name, Operator.Equal, "Foo", Formatter.AndClauseSeparator);
            query.Parameters.Count.ShouldBe(1);
            query.Parameters["Name1"].ShouldBe("Foo");

            var sql = query.Compile(Table.MakeOrGet<Person>(Dialect.Generic).Delete);
            sql.ShouldBe(@"DELETE FROM [Person]
WHERE
    1 = 1
AND
    ([Name]=@Name1);");
        }

        [Test]
        public void When_adding_filter_to_a_update_statement()
        {
            var query = FilteredQuery.Make<Person>();
            query.AddClause<Person, string>(p => p.Name, Operator.Equal, "Foo", Formatter.AndClauseSeparator);
            query.Parameters.Count.ShouldBe(1);
            query.Parameters["Name1"].ShouldBe("Foo");

            var sql = query.Compile(Table.MakeOrGet<Person>(Dialect.Generic).UpdateCustom);
            sql.ShouldBe(@"UPDATE [Person] SET
    [Name] = @Name,
    [Age] = @Age
WHERE
    1 = 1
AND
    ([Name]=@Name1);");
        }
    }
}