namespace Easy.Storage.SQLServer.Extensions
{
    using System;
    using Easy.Common;
    using Easy.Storage.SQLServer.Models;

    internal static class StringExtensions
    {
        // ReSharper disable once InconsistentNaming
        internal static SQLServerDataType ParseAsSQLServerDataType(this string columnType)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(columnType);

            const StringComparison Policy = StringComparison.OrdinalIgnoreCase;
            if (columnType.Equals("image", Policy))
            {
                return SQLServerDataType.Image;
            }

            if (columnType.Equals("text", Policy))
            {
                return SQLServerDataType.Text;
            }

            if (columnType.Equals("uniqueidentifier", Policy))
            {
                return SQLServerDataType.UniqueIdentifier;
            }

            if (columnType.Equals("date", Policy))
            {
                return SQLServerDataType.Date;
            }

            if (columnType.Equals("time", Policy))
            {
                return SQLServerDataType.Time;
            }

            if (columnType.Equals("smalldatetime", Policy))
            {
                return SQLServerDataType.SmallDateTime;
            }

            if (columnType.Equals("datetime", Policy))
            {
                return SQLServerDataType.DateTime;
            }

            if (columnType.Equals("datetime2", Policy))
            {
                return SQLServerDataType.DateTime2;
            }

            if (columnType.Equals("datetimeoffset", Policy))
            {
                return SQLServerDataType.DateTimeOffset;
            }

            if (columnType.Equals("timestamp", Policy))
            {
                return SQLServerDataType.Timestamp;
            }

            if (columnType.Equals("tinyint", Policy))
            {
                return SQLServerDataType.TinyInt;
            }

            if (columnType.Equals("smallint", Policy))
            {
                return SQLServerDataType.SmallInt;
            }

            if (columnType.Equals("int", Policy))
            {
                return SQLServerDataType.Int;
            }

            if (columnType.Equals("real", Policy))
            {
                return SQLServerDataType.Real;
            }

            if (columnType.Equals("money", Policy))
            {
                return SQLServerDataType.Money;
            }

            if (columnType.Equals("float", Policy))
            {
                return SQLServerDataType.Float;
            }

            if (columnType.Equals("decimal", Policy))
            {
                return SQLServerDataType.Decimal;
            }

            if (columnType.Equals("sql_variant", Policy))
            {
                return SQLServerDataType.Variant;
            }

            if (columnType.Equals("ntext", Policy))
            {
                return SQLServerDataType.NText;
            }

            if (columnType.Equals("bit", Policy))
            {
                return SQLServerDataType.Bit;
            }

            if (columnType.Equals("numeric", Policy))
            {
                return SQLServerDataType.Numeric;
            }

            if (columnType.Equals("smallmoney", Policy))
            {
                return SQLServerDataType.SmallMoney;
            }

            if (columnType.Equals("bigint", Policy))
            {
                return SQLServerDataType.BigInt;
            }

            if (columnType.Equals("hierarchyid", Policy))
            {
                return SQLServerDataType.HierarchyId;
            }

            if (columnType.Equals("geometry", Policy))
            {
                return SQLServerDataType.Geometry;
            }

            if (columnType.Equals("geography", Policy))
            {
                return SQLServerDataType.Geography;
            }

            if (columnType.Equals("varbinary", Policy))
            {
                return SQLServerDataType.VarBinary;
            }

            if (columnType.Equals("varchar", Policy))
            {
                return SQLServerDataType.VarChar;
            }

            if (columnType.Equals("binary", Policy))
            {
                return SQLServerDataType.Binary;
            }

            if (columnType.Equals("char", Policy))
            {
                return SQLServerDataType.Char;
            }

            if (columnType.Equals("nvarchar", Policy))
            {
                return SQLServerDataType.NVarChar;
            }

            if (columnType.Equals("nchar", Policy))
            {
                return SQLServerDataType.NChar;
            }

            if (columnType.Equals("xml", Policy))
            {
                return SQLServerDataType.Xml;
            }

            if (columnType.Equals("sysname", Policy))
            {
                return SQLServerDataType.SysName;
            }

            throw new ArgumentOutOfRangeException(nameof(columnType), "Invalid column type of: " + columnType);
        }
    }
}