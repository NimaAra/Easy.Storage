namespace Easy.Storage.Tests.Unit.Filter
{
    using Easy.Storage.Common;
    using Easy.Storage.Common.Filter;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class FilterBuilderTests
    {
        private static readonly Table _table = 
            Table.MakeOrGet<PersonWithType>(GenericSQLDialect.Instance, string.Empty);

        [Test]
        public void When_creating_a_builder()
        {
            var filter = FilterBuilder.For(_table);
            filter.ShouldNotBeNull();
            filter.Parameters.ShouldBeEmpty();

            var sql = filter.Compile();
            sql.ShouldBe(@"SELECT
    [Person].[Type] AS 'Type',
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
            var filter = FilterBuilder.For(_table);
            filter.AddClause<PersonWithType, long>(p => p.Id, Operator.Is, 1, Formatter.AndClauseSeparator);
            filter.Parameters.Count.ShouldBe(1);
            filter.Parameters["Id1"].ShouldBe(1);

            var sql = filter.Compile();
            sql.ShouldBe(@"SELECT
    [Person].[Type] AS 'Type',
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
            var filter = FilterBuilder.For(_table);
            filter.AddInClause<PersonWithType, string>(p => p.Name, Formatter.AndClauseSeparator, true, new [] { "Foo", "Bar" });
            filter.Parameters.Count.ShouldBe(1);
            filter.Parameters["Name1"].ShouldBe(new [] { "Foo", "Bar"});

            var sql = filter.Compile();
            sql.ShouldBe(@"SELECT
    [Person].[Type] AS 'Type',
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
            var filter = FilterBuilder.For(_table);
            filter.AddInClause<PersonWithType, string>(p => p.Name, Formatter.AndClauseSeparator, false, new[] { "Foo", "Bar" });
            filter.Parameters.Count.ShouldBe(1);
            filter.Parameters["Name1"].ShouldBe(new[] { "Foo", "Bar" });

            var sql = filter.Compile();
            sql.ShouldBe(@"SELECT
    [Person].[Type] AS 'Type',
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
            var filter = FilterBuilder.For(_table);
            filter.AddClause<PersonWithType, string>(p => p.Name, Operator.Is, "Foo", Formatter.AndClauseSeparator);
            filter.Parameters.Count.ShouldBe(1);
            filter.Parameters["Name1"].ShouldBe("Foo");

            var sql = filter.Compile(
                Table.MakeOrGet<PersonWithType>(GenericSQLDialect.Instance, string.Empty).Delete);
            sql.ShouldBe(@"DELETE FROM [Person]
WHERE
    1 = 1
AND
    ([Name]=@Name1);");
        }

        [Test]
        public void When_adding_filter_to_a_update_statement()
        {
            var filter = FilterBuilder.For(_table);
            filter.AddClause<PersonWithType, string>(p => p.Name, Operator.Is, "Foo", Formatter.AndClauseSeparator);
            filter.Parameters.Count.ShouldBe(1);
            filter.Parameters["Name1"].ShouldBe("Foo");

            var sql = filter.Compile(
                Table.MakeOrGet<Person>(GenericSQLDialect.Instance, string.Empty).UpdateAll);
            sql.ShouldBe(@"UPDATE [Person] SET
    [Id] = @Id,
    [Name] = @Name,
    [Age] = @Age
WHERE
    1 = 1
AND
    ([Name]=@Name1);");
        }

        [Test]
        public void When_filter_has_int_as_parameter()
        {
            var filter = Query<PersonWithType>.For(_table)
                .Filter.And(x => x.Id, Operator.Is, 1);

            filter.Parameters["Id1"].ShouldBe(1);
        }

        [Test]
        public void When_filter_has_string_as_parameter()
        {
            var filter = Query<PersonWithType>.For(_table)
                .Filter.And(x => x.Name, Operator.Is, "Foo");

            filter.Parameters["Name1"].ShouldBe("Foo");
        }

        [Test]
        public void When_filter_has_enum_as_parameter()
        {
            var filter = Query<PersonWithType>.For(_table)
                .Filter.And(x => x.Type, Operator.Is, SomeType.TypeB);

            filter.Parameters["Type1"].ShouldBe("TypeB");
        }

        [Test]
        public void When_filter_has_array_of_enums_as_parameter()
        {
            var filter = Query<PersonWithType>.For(_table)
                .Filter.AndIn(x => x.Type, new[] { SomeType.TypeA, SomeType.TypeB });

            filter.Parameters["Type1"].ShouldBe(new[] { "TypeA", "TypeB" });
        }

        [Test]
        public void When_filter_has_array_of_numbers_as_parameter()
        {
            var filter = Query<PersonWithType>.For(_table)
                .Filter.AndIn(x => x.Id, new long[] { 1, 2 });

            filter.Parameters["Id1"].ShouldBe(new[] { 1, 2 });
        }
    }
}