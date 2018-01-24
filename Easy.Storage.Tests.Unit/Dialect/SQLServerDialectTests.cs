namespace Easy.Storage.Tests.Unit.Dialect
{
    using System;
    using System.IO;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Filter;
    using Easy.Storage.SQLServer;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class SQLServerDialectTests
    {
        [Test]
        public void When_creating_a_sqlserver_dialect()
        {
            var instanceOne = SQLServerDialect.Instance;
            var instanceTwo = SQLServerDialect.Instance;

            instanceOne.ShouldBe(instanceTwo);
            instanceOne.ShouldBeSameAs(instanceTwo);

            SQLServerDialect.Instance.Type.ShouldBe(DialectType.SQLServer);
        }

        [Test]
        public void When_getting_partial_update_query()
        {
            var dialect = SQLServerDialect.Instance;
            var table = Table.MakeOrGet<Person>(dialect, string.Empty);
            var filter = Query<Person>.Filter.And(x => x.Id, Operator.Equal, 1);

            var person = new Person { Age = 10, Name = "Joe"};
            dialect.GetPartialUpdateQuery(table, person, filter)
                .ShouldBe(@"UPDATE [Person] SET
    [Id] = @Id,
    [Name] = @Name,
    [Age] = @Age
WHERE 1=1
AND
    ([Id]=@Id1);");

            var item = new { Age = 20, Name = "Bob"};
            dialect.GetPartialUpdateQuery(table, item, filter)
                .ShouldBe(@"UPDATE [Person] SET
    [Age] = @Age,
    [Name] = @Name
WHERE 1=1
AND
    ([Id]=@Id1);");

            var lonelyTable = Table.MakeOrGet<Lonely>(dialect, string.Empty);
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
        public void When_getting_partial_insert_query()
        {
            var dialect = SQLServerDialect.Instance;
            var table = Table.MakeOrGet<Person>(dialect, string.Empty);

            var itemWithAllProperties = new { Id = 1, Name = "Foo", Age = 123 };
            dialect.GetPartialInsertQuery<Person>(table, itemWithAllProperties)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id BIGINT);
INSERT INTO [Person]
(
    [Id],
    [Name],
    [Age]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Id,
    @Name,
    @Age
);
SELECT Id FROM @InsertedRows;");

            var itemWithSomeProperties = new { Name = "Foo" };
            dialect.GetPartialInsertQuery<Person>(table, itemWithSomeProperties)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id BIGINT);
INSERT INTO [Person]
(
    [Name]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Name
);
SELECT Id FROM @InsertedRows;");

            var itemWithNoProperties = new { };
            Should.Throw<InvalidDataException>(() => dialect.GetPartialInsertQuery<Person>(table, itemWithNoProperties))
                .Message.ShouldBe("Unable to find any properties in: item");

            var lonelyTable = Table.MakeOrGet<Lonely>(dialect, string.Empty);
            var lonely = new Lonely { Id = 1 };
            dialect.GetPartialInsertQuery<Lonely>(lonelyTable, lonely)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id BIGINT);
INSERT INTO [Lonely]
(
    [Id]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Id
);
SELECT Id FROM @InsertedRows;");
        }

        [Test]
        public void When_getting_insert_query_single_property()
        {
            var dialect = SQLServerDialect.Instance;
            var lonelyTable = Table.MakeOrGet<Lonely>(dialect, string.Empty);
            dialect.GetInsertQuery(lonelyTable, true)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id BIGINT);
INSERT INTO [Lonely]
(
    [Id]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Id
);
SELECT Id FROM @InsertedRows;");

            dialect.GetInsertQuery(lonelyTable, false)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id BIGINT);
INSERT INTO [Lonely]
(
    
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    
);
SELECT Id FROM @InsertedRows;");
        }

        [Test]
        public void When_getting_insert_query_with_string_identity()
        {
            var dialect = SQLServerDialect.Instance;
            var table = Table.MakeOrGet<_ClassWithStringId>(dialect, string.Empty);
            dialect.GetInsertQuery(table, true)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id NVARCHAR(MAX));
INSERT INTO [_ClassWithStringId]
(
    [Id]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Id
);
SELECT Id FROM @InsertedRows;");

            dialect.GetInsertQuery(table, false)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id NVARCHAR(MAX));
INSERT INTO [_ClassWithStringId]
(
    
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    
);
SELECT Id FROM @InsertedRows;");
        }

        [Test]
        public void When_getting_insert_query_with_byte_identity()
        {
            var dialect = SQLServerDialect.Instance;
            var table = Table.MakeOrGet<_ClassWithByteId>(dialect, string.Empty);
            dialect.GetInsertQuery(table, true)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id TINYINT);
INSERT INTO [_ClassWithByteId]
(
    [Id]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Id
);
SELECT Id FROM @InsertedRows;");

            dialect.GetInsertQuery(table, false)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id TINYINT);
INSERT INTO [_ClassWithByteId]
(
    
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    
);
SELECT Id FROM @InsertedRows;");
        }

        [Test]
        public void When_getting_insert_query_with_short_identity()
        {
            var dialect = SQLServerDialect.Instance;
            var table = Table.MakeOrGet<_ClassWithShortId>(dialect, string.Empty);
            dialect.GetInsertQuery(table, true)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id SMALLINT);
INSERT INTO [_ClassWithShortId]
(
    [Id]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Id
);
SELECT Id FROM @InsertedRows;");

            dialect.GetInsertQuery(table, false)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id SMALLINT);
INSERT INTO [_ClassWithShortId]
(
    
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    
);
SELECT Id FROM @InsertedRows;");
        }

        [Test]
        public void When_getting_insert_query_with_int_identity()
        {
            var dialect = SQLServerDialect.Instance;
            var table = Table.MakeOrGet<_ClassWithIntId>(dialect, string.Empty);
            dialect.GetInsertQuery(table, true)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id INT);
INSERT INTO [_ClassWithIntId]
(
    [Id]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Id
);
SELECT Id FROM @InsertedRows;");

            dialect.GetInsertQuery(table, false)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id INT);
INSERT INTO [_ClassWithIntId]
(
    
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    
);
SELECT Id FROM @InsertedRows;");
        }

        [Test]
        public void When_getting_insert_query_with_long_identity()
        {
            var dialect = SQLServerDialect.Instance;
            var table = Table.MakeOrGet<_ClassWithLongId>(dialect, string.Empty);
            dialect.GetInsertQuery(table, true)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id BIGINT);
INSERT INTO [_ClassWithLongId]
(
    [Id]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Id
);
SELECT Id FROM @InsertedRows;");

            dialect.GetInsertQuery(table, false)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id BIGINT);
INSERT INTO [_ClassWithLongId]
(
    
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    
);
SELECT Id FROM @InsertedRows;");
        }

        [Test]
        public void When_getting_insert_query_with_guid_identity()
        {
            var dialect = SQLServerDialect.Instance;
            var table = Table.MakeOrGet<_ClassWithGuidId>(dialect, string.Empty);
            dialect.GetInsertQuery(table, true)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id UNIQUEIDENTIFIER);
INSERT INTO [_ClassWithGuidId]
(
    [Id]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Id
);
SELECT Id FROM @InsertedRows;");

            dialect.GetInsertQuery(table, false)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id UNIQUEIDENTIFIER);
INSERT INTO [_ClassWithGuidId]
(
    
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    
);
SELECT Id FROM @InsertedRows;");
        }

        [Test]
        public void When_getting_insert_query_with_bool_identity()
        {
            var dialect = SQLServerDialect.Instance;
            var table = Table.MakeOrGet<_ClassWithBoolId>(dialect, string.Empty);
            dialect.GetInsertQuery(table, true)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id BIT);
INSERT INTO [_ClassWithBoolId]
(
    [Id]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Id
);
SELECT Id FROM @InsertedRows;");

            dialect.GetInsertQuery(table, false)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id BIT);
INSERT INTO [_ClassWithBoolId]
(
    
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    
);
SELECT Id FROM @InsertedRows;");
        }

        [Test]
        public void When_getting_insert_query_with_bytes_identity()
        {
            var dialect = SQLServerDialect.Instance;
            var table = Table.MakeOrGet<_ClassWithBytesId>(dialect, string.Empty);
            dialect.GetInsertQuery(table, true)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id VARBINARY);
INSERT INTO [_ClassWithBytesId]
(
    [Id]
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    @Id
);
SELECT Id FROM @InsertedRows;");

            dialect.GetInsertQuery(table, false)
                .ShouldBe(@"DECLARE @InsertedRows AS TABLE (Id VARBINARY);
INSERT INTO [_ClassWithBytesId]
(
    
) OUTPUT Inserted.[Id] INTO @InsertedRows
VALUES
(
    
);
SELECT Id FROM @InsertedRows;");
        }

        [Test]
        public void When_getting_insert_query_with_invalid_identity()
        {
            var dialect = SQLServerDialect.Instance;
            Should.Throw<NotSupportedException>(() => Table.MakeOrGet<_ClassWithInvalidId>(dialect, string.Empty))
                .Message.ShouldBe("The requested identity column of: Id with the type: TimeSpan is not supported.");
        }

        private sealed class _ClassWithStringId { public string Id { get; set; } }
        private sealed class _ClassWithByteId { public byte Id { get; set; } }
        private sealed class _ClassWithShortId { public short Id { get; set; } }
        private sealed class _ClassWithIntId { public int Id { get; set; } }
        private sealed class _ClassWithLongId { public long Id { get; set; } }
        private sealed class _ClassWithGuidId { public Guid Id { get; set; } }
        private sealed class _ClassWithBoolId { public bool Id { get; set; } }
        private sealed class _ClassWithBytesId { public byte[] Id { get; set; } }
        private sealed class _ClassWithInvalidId { public TimeSpan Id { get; set; } }
    }
}