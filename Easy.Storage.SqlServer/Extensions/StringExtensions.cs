namespace Easy.Storage.SqlServer.Extensions
{
    using System;
    using Easy.Storage.SqlServer.Models;

    internal static class StringExtensions
    {
        internal static SqlServerDataType ParseAsSqlServerDataType(this string columnType)
        {
            var policy = StringComparison.OrdinalIgnoreCase;
            if (columnType.Equals("image", policy))
            {
                return SqlServerDataType.Image;
            }

            if (columnType.Equals("text", policy))
            {
                return SqlServerDataType.Text;
            }

            if (columnType.Equals("uniqueidentifier", policy))
            {
                return SqlServerDataType.UniqueIdentifier;
            }

            if (columnType.Equals("date", policy))
            {
                return SqlServerDataType.Date;
            }

            if (columnType.Equals("time", policy))
            {
                return SqlServerDataType.Time;
            }

            if (columnType.Equals("smalldatetime", policy))
            {
                return SqlServerDataType.SmallDateTime;
            }

            if (columnType.Equals("datetime", policy))
            {
                return SqlServerDataType.DateTime;
            }

            if (columnType.Equals("datetime2", policy))
            {
                return SqlServerDataType.DateTime2;
            }

            if (columnType.Equals("datetimeoffset", policy))
            {
                return SqlServerDataType.DateTimeOffset;
            }

            if (columnType.Equals("tinyint", policy))
            {
                return SqlServerDataType.TinyInt;
            }

            if (columnType.Equals("smallint", policy))
            {
                return SqlServerDataType.SmallInt;
            }

            if (columnType.Equals("int", policy))
            {
                return SqlServerDataType.Int;
            }

            if (columnType.Equals("real", policy))
            {
                return SqlServerDataType.Real;
            }

            if (columnType.Equals("money", policy))
            {
                return SqlServerDataType.Money;
            }

            if (columnType.Equals("float", policy))
            {
                return SqlServerDataType.Float;
            }

            if (columnType.Equals("decimal", policy))
            {
                return SqlServerDataType.Decimal;
            }

            if (columnType.Equals("sql_variant", policy))
            {
                return SqlServerDataType.Variant;
            }

            if (columnType.Equals("ntext", policy))
            {
                return SqlServerDataType.NText;
            }

            if (columnType.Equals("bit", policy))
            {
                return SqlServerDataType.Bit;
            }

            if (columnType.Equals("numeric", policy))
            {
                return SqlServerDataType.Numeric;
            }

            if (columnType.Equals("smallmoney", policy))
            {
                return SqlServerDataType.SmallMoney;
            }

            if (columnType.Equals("bigint", policy))
            {
                return SqlServerDataType.BigInt;
            }

            if (columnType.Equals("hierarchyid", policy))
            {
                return SqlServerDataType.HierarchyId;
            }

            if (columnType.Equals("geometry", policy))
            {
                return SqlServerDataType.Geometry;
            }

            if (columnType.Equals("geography", policy))
            {
                return SqlServerDataType.Geography;
            }

            if (columnType.Equals("varbinary", policy))
            {
                return SqlServerDataType.VarBinary;
            }

            if (columnType.Equals("varchar", policy))
            {
                return SqlServerDataType.VarChar;
            }

            if (columnType.Equals("binary", policy))
            {
                return SqlServerDataType.Binary;
            }

            if (columnType.Equals("char", policy))
            {
                return SqlServerDataType.Char;
            }

            if (columnType.Equals("timestamp", policy))
            {
                return SqlServerDataType.Timestamp;
            }

            if (columnType.Equals("nvarchar", policy))
            {
                return SqlServerDataType.NVarChar;
            }

            if (columnType.Equals("nchar", policy))
            {
                return SqlServerDataType.NChar;
            }

            if (columnType.Equals("xml", policy))
            {
                return SqlServerDataType.Xml;
            }

            if (columnType.Equals("sysname", policy))
            {
                return SqlServerDataType.SysName;
            }

            throw new ArgumentOutOfRangeException(nameof(columnType), "Invalid column type of: " + columnType);
        }
    }
}