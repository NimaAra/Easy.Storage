namespace Easy.Storage.Tests.Unit.SqlServer
{
    using System;
    using Easy.Storage.SqlServer.Models;
    using NUnit.Framework;
    using Shouldly;
    using Storage.SqlServer.Extensions;

    [TestFixture]
    internal sealed class SqlServerColumnTypeParsingTests
    {
        [Test]
        public void When_parsing_null()
        {
            Should.Throw<ArgumentException>(() => ((string)null).ParseAsSqlServerDataType())
                .Message.ShouldBe("String must not be null, empty or whitespace.");
        }

        [Test]
        public void When_parsing_empty_string()
        {
            var e = Should.Throw<ArgumentException>(() => string.Empty.ParseAsSqlServerDataType());
            e.Message.ShouldBe("String must not be null, empty or whitespace.");
        }

        [Test]
        public void When_parsing_whitespace_string()
        {
            var e = Should.Throw<ArgumentException>(() => " ".ParseAsSqlServerDataType());
            e.Message.ShouldBe("String must not be null, empty or whitespace.");
        }

        [Test]
        public void When_parsing_invalid_string()
        {
            var e = Should.Throw<ArgumentOutOfRangeException>(() => "foo".ParseAsSqlServerDataType());
            e.ParamName.ShouldBe("columnType");
        }

        [Test]
        public void When_parsing_valid_strings()
        {
            "image".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Image);
            "IMAGE".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Image);
            "iMaGe".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Image);

            "text".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Text);
            "uniqueidentifier".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.UniqueIdentifier);
            "date".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Date);
            "time".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Time);
            "smalldatetime".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.SmallDateTime);
            "datetime".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.DateTime);
            "datetime2".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.DateTime2);
            "datetimeoffset".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.DateTimeOffset);
            "timestamp".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Timestamp);
            "tinyint".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.TinyInt);
            "smallint".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.SmallInt);
            "int".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Int);
            "real".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Real);
            "money".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Money);
            "float".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Float);
            "decimal".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Decimal);
            "sql_variant".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Variant);
            "ntext".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.NText);
            "bit".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Bit);
            "numeric".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Numeric);
            "smallmoney".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.SmallMoney);
            "bigint".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.BigInt);
            "hierarchyid".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.HierarchyId);
            "geometry".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Geometry);
            "geography".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Geography);
            "varbinary".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.VarBinary);
            "varchar".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.VarChar);
            "binary".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Binary);
            "char".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Char);
            "nvarchar".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.NVarChar);
            "nchar".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.NChar);
            "xml".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.Xml);
            "sysname".ParseAsSqlServerDataType().ShouldBe(SqlServerDataType.SysName);
        }
    }
}