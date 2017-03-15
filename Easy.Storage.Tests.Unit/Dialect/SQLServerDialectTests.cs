namespace Easy.Storage.Tests.Unit.Dialect
{
    using System;
    using Easy.Storage.Common;
    using Easy.Storage.SQLServer;
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
        public void When_getting_insert_query_with_string_identity()
        {
            var dialect = SQLServerDialect.Instance;
            var table = Table.MakeOrGet<_ClassWithStringId>(dialect);
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
            var table = Table.MakeOrGet<_ClassWithByteId>(dialect);
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
            var table = Table.MakeOrGet<_ClassWithShortId>(dialect);
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
            var table = Table.MakeOrGet<_ClassWithIntId>(dialect);
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
            var table = Table.MakeOrGet<_ClassWithLongId>(dialect);
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
            var table = Table.MakeOrGet<_ClassWithGuidId>(dialect);
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
            var table = Table.MakeOrGet<_ClassWithBoolId>(dialect);
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
            var table = Table.MakeOrGet<_ClassWithBytesId>(dialect);
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
            Should.Throw<NotSupportedException>(() => Table.MakeOrGet<_ClassWithInvalidId>(dialect))
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