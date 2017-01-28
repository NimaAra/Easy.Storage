namespace Easy.Storage.Tests.Unit.SQLServer
{
    using System;
    using Easy.Storage.SQLServer.Extensions;
    using Easy.Storage.SQLServer.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class SQLServerColumnTypeParsingTests
    {
        [Test]
        public void When_parsing_null()
        {
            Should.Throw<ArgumentException>(() => ((string)null).ParseAsSQLServerDataType())
                .Message.ShouldBe("String must not be null, empty or whitespace.");
        }

        [Test]
        public void When_parsing_empty_string()
        {
            var e = Should.Throw<ArgumentException>(() => string.Empty.ParseAsSQLServerDataType());
            e.Message.ShouldBe("String must not be null, empty or whitespace.");
        }

        [Test]
        public void When_parsing_whitespace_string()
        {
            var e = Should.Throw<ArgumentException>(() => " ".ParseAsSQLServerDataType());
            e.Message.ShouldBe("String must not be null, empty or whitespace.");
        }

        [Test]
        public void When_parsing_invalid_string()
        {
            var e = Should.Throw<ArgumentOutOfRangeException>(() => "foo".ParseAsSQLServerDataType());
            e.ParamName.ShouldBe("columnType");
        }

        [Test]
        public void When_parsing_valid_strings()
        {
            "image".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Image);
            "IMAGE".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Image);
            "iMaGe".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Image);

            "text".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Text);
            "uniqueidentifier".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.UniqueIdentifier);
            "date".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Date);
            "time".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Time);
            "smalldatetime".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.SmallDateTime);
            "datetime".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.DateTime);
            "datetime2".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.DateTime2);
            "datetimeoffset".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.DateTimeOffset);
            "timestamp".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Timestamp);
            "tinyint".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.TinyInt);
            "smallint".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.SmallInt);
            "int".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Int);
            "real".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Real);
            "money".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Money);
            "float".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Float);
            "decimal".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Decimal);
            "sql_variant".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Variant);
            "ntext".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.NText);
            "bit".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Bit);
            "numeric".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Numeric);
            "smallmoney".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.SmallMoney);
            "bigint".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.BigInt);
            "hierarchyid".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.HierarchyId);
            "geometry".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Geometry);
            "geography".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Geography);
            "varbinary".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.VarBinary);
            "varchar".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.VarChar);
            "binary".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Binary);
            "char".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Char);
            "nvarchar".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.NVarChar);
            "nchar".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.NChar);
            "xml".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.Xml);
            "sysname".ParseAsSQLServerDataType().ShouldBe(SQLServerDataType.SysName);
        }
    }
}