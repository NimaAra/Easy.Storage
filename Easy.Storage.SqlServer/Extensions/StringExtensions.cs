namespace Easy.Storage.SqlServer.Extensions
{
    using System;
    using Easy.Common;
    using Easy.Storage.SqlServer.Models;

    internal static class StringExtensions
    {
        internal static SqlServerDataType ParseAsSqlServerDataType(this string columnType)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(columnType);

            const StringComparison Policy = StringComparison.OrdinalIgnoreCase;
            if (columnType.Equals("image", Policy))
            {
                return SqlServerDataType.Image;
            }

            if (columnType.Equals("text", Policy))
            {
                return SqlServerDataType.Text;
            }

            if (columnType.Equals("uniqueidentifier", Policy))
            {
                return SqlServerDataType.UniqueIdentifier;
            }

            if (columnType.Equals("date", Policy))
            {
                return SqlServerDataType.Date;
            }

            if (columnType.Equals("time", Policy))
            {
                return SqlServerDataType.Time;
            }

            if (columnType.Equals("smalldatetime", Policy))
            {
                return SqlServerDataType.SmallDateTime;
            }

            if (columnType.Equals("datetime", Policy))
            {
                return SqlServerDataType.DateTime;
            }

            if (columnType.Equals("datetime2", Policy))
            {
                return SqlServerDataType.DateTime2;
            }

            if (columnType.Equals("datetimeoffset", Policy))
            {
                return SqlServerDataType.DateTimeOffset;
            }

            if (columnType.Equals("timestamp", Policy))
            {
                return SqlServerDataType.Timestamp;
            }

            if (columnType.Equals("tinyint", Policy))
            {
                return SqlServerDataType.TinyInt;
            }

            if (columnType.Equals("smallint", Policy))
            {
                return SqlServerDataType.SmallInt;
            }

            if (columnType.Equals("int", Policy))
            {
                return SqlServerDataType.Int;
            }

            if (columnType.Equals("real", Policy))
            {
                return SqlServerDataType.Real;
            }

            if (columnType.Equals("money", Policy))
            {
                return SqlServerDataType.Money;
            }

            if (columnType.Equals("float", Policy))
            {
                return SqlServerDataType.Float;
            }

            if (columnType.Equals("decimal", Policy))
            {
                return SqlServerDataType.Decimal;
            }

            if (columnType.Equals("sql_variant", Policy))
            {
                return SqlServerDataType.Variant;
            }

            if (columnType.Equals("ntext", Policy))
            {
                return SqlServerDataType.NText;
            }

            if (columnType.Equals("bit", Policy))
            {
                return SqlServerDataType.Bit;
            }

            if (columnType.Equals("numeric", Policy))
            {
                return SqlServerDataType.Numeric;
            }

            if (columnType.Equals("smallmoney", Policy))
            {
                return SqlServerDataType.SmallMoney;
            }

            if (columnType.Equals("bigint", Policy))
            {
                return SqlServerDataType.BigInt;
            }

            if (columnType.Equals("hierarchyid", Policy))
            {
                return SqlServerDataType.HierarchyId;
            }

            if (columnType.Equals("geometry", Policy))
            {
                return SqlServerDataType.Geometry;
            }

            if (columnType.Equals("geography", Policy))
            {
                return SqlServerDataType.Geography;
            }

            if (columnType.Equals("varbinary", Policy))
            {
                return SqlServerDataType.VarBinary;
            }

            if (columnType.Equals("varchar", Policy))
            {
                return SqlServerDataType.VarChar;
            }

            if (columnType.Equals("binary", Policy))
            {
                return SqlServerDataType.Binary;
            }

            if (columnType.Equals("char", Policy))
            {
                return SqlServerDataType.Char;
            }

            if (columnType.Equals("nvarchar", Policy))
            {
                return SqlServerDataType.NVarChar;
            }

            if (columnType.Equals("nchar", Policy))
            {
                return SqlServerDataType.NChar;
            }

            if (columnType.Equals("xml", Policy))
            {
                return SqlServerDataType.Xml;
            }

            if (columnType.Equals("sysname", Policy))
            {
                return SqlServerDataType.SysName;
            }

            throw new ArgumentOutOfRangeException(nameof(columnType), "Invalid column type of: " + columnType);
        }
    }
}