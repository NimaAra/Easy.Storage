// ReSharper disable InconsistentNaming
namespace Easy.Storage.SQLite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using Easy.Common;
    using Easy.Common.Extensions;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Attributes;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.SQLite.Models;

    /// <summary>
    /// A handy class to generate <c>SQLite</c> table scripts from a model.
    /// </summary>
    public static class SQLiteSQLGenerator
    {
        private const string PrimaryKey = " PRIMARY KEY";
        private const string NotNull = " NOT NULL";

        /// <summary>
        /// Returns a <c>CREATE TABLE</c> script for the given <typeparamref name="T"/>.
        /// </summary>
        public static string Table<T>()
        {
            var table = Common.Table.MakeOrGet<T>(SQLiteDialect.Instance, string.Empty);
            var tableName = table.Name.GetNameFromEscapedSQLName();
            return TableImpl(table, tableName);
        }

        /// <summary>
        /// Returns a <c>CREATE TABLE</c> script for the given <paramref name="tableName"/>
        /// mapped to the given <typeparamref name="T"/> model.
        /// </summary>
        public static string Table<T>(string tableName)
        {
            var table = Common.Table.MakeOrGet<T>(SQLiteDialect.Instance, tableName);
            var name = table.Name.GetNameFromEscapedSQLName();
            return TableImpl(table, name);
        }

        /// <summary>
        /// Returns a <c>CREATE TABLE</c> script for the given <typeparamref name="T"/>.
        /// </summary>
        // ReSharper disable once InconsistentNaming
        public static string FTSTable<T>(FTSTableType type, params Expression<Func<T, object>>[] selector)
        {
            var table = Common.Table.MakeOrGet<T>(SQLiteDialect.Instance, string.Empty);
            var propNameToColumn = table.PropertyToColumns.ToDictionary(kv => kv.Key.Name, kv => kv.Value);

            var columns = new List<string>();

            if (!selector.Any())
            {
                columns = propNameToColumn.Values.ToList();
            } else
            {
                foreach (var item in selector)
                {
                    var propName = item.GetPropertyName();

                    if (propNameToColumn.TryGetValue(propName, out string column))
                    {
                        columns.Add(column);
                    }
                    else
                    {
                        throw new KeyNotFoundException($"Could not find a mapping for property: {propName}. Ensure it is not marked with an {nameof(IgnoreAttribute)}.");
                    }
                }
            }

            var tableName = table.Name.GetNameFromEscapedSQLName();
            var ftsTableName = string.Concat(tableName, "_fts");
            var ftsColumns = string.Join(", ", columns);

            switch (type)
            {
                case FTSTableType.Content:
                    return $"CREATE VIRTUAL TABLE IF NOT EXISTS {ftsTableName} USING FTS4 ({ftsColumns});";
                case FTSTableType.ContentLess:
                    return $"CREATE VIRTUAL TABLE IF NOT EXISTS {ftsTableName} USING FTS4 (content=\"\", {ftsColumns});";
                case FTSTableType.ExternalContent:
                    var builder = StringBuilderCache.Acquire();
                    builder.AppendLine($"CREATE VIRTUAL TABLE IF NOT EXISTS {ftsTableName} USING FTS4 (content=\"{tableName}\", {ftsColumns});");
                    builder.AppendLine();

                    var ftsTriggerColumns = string.Join(", ", columns.Select(c => "new." + c));
                    builder.AppendLine($"CREATE TRIGGER IF NOT EXISTS {tableName}_bu BEFORE UPDATE ON {tableName} BEGIN");
                    builder.AppendLine($"{Formatter.Spacer}DELETE FROM {ftsTableName} WHERE docId = old.rowId;");
                    builder.AppendLine("END;");
                    builder.AppendLine();
                    builder.AppendLine($"CREATE TRIGGER IF NOT EXISTS {tableName}_bd BEFORE DELETE ON {tableName} BEGIN");
                    builder.AppendLine($"{Formatter.Spacer}DELETE FROM {ftsTableName} WHERE docId = old.rowId;");
                    builder.AppendLine("END;");
                    builder.AppendLine();
                    builder.AppendLine($"CREATE TRIGGER IF NOT EXISTS {tableName}_au AFTER UPDATE ON {tableName} BEGIN");
                    builder.AppendLine($"{Formatter.Spacer}INSERT INTO {ftsTableName} (docId, {ftsColumns}) VALUES (new.rowId, {ftsTriggerColumns});");
                    builder.AppendLine("END;");
                    builder.AppendLine();
                    builder.AppendLine($"CREATE TRIGGER IF NOT EXISTS {tableName}_ai AFTER INSERT ON {tableName} BEGIN");
                    builder.AppendLine($"{Formatter.Spacer}INSERT INTO {ftsTableName} (docId, {ftsColumns}) VALUES (new.rowId, {ftsTriggerColumns});");
                    builder.Append("END;");
                    return StringBuilderCache.GetStringAndRelease(builder);
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        private static string TableImpl(Table table, string tableName)
        {
            var builder = StringBuilderCache.Acquire();
            builder.AppendLine($"CREATE TABLE IF NOT EXISTS {tableName} (");
            builder.AppendLine($"{Formatter.Spacer}[_Entry_TimeStamp_Epoch_ms_] INTEGER DEFAULT (CAST((julianday('now') - 2440587.5)*86400000 AS INTEGER)),");

            foreach (var pair in table.PropertyToColumns)
            {
                var sqliteType = GetSQLiteType(pair.Key.PropertyType).ToString();
                var isIdColumn = table.IdentityColumn == pair.Key;

                builder.AppendLine($"{Formatter.Spacer}{pair.Value} {sqliteType}{(isIdColumn? PrimaryKey : "")}{NotNull},");
            }

            builder.Remove(builder.Length - 3, 1);
            builder.Append(");");

            return StringBuilderCache.GetStringAndRelease(builder);
        }

        private static SQLiteDataType GetSQLiteType(Type type)
        {
            if (type.IsEnum) { return SQLiteDataType.TEXT; }

            if (type == ClrTypes.Bool || type == ClrTypes.BoolNull) { return SQLiteDataType.INTEGER; }
            if (type == ClrTypes.Byte || type == ClrTypes.ByteNull) { return SQLiteDataType.INTEGER; }
            if (type == ClrTypes.Short || type == ClrTypes.ShortNull) { return SQLiteDataType.INTEGER; }
            if (type == ClrTypes.UShort || type == ClrTypes.UShortNull) { return SQLiteDataType.INTEGER; }
            if (type == ClrTypes.Int || type == ClrTypes.IntNull) { return SQLiteDataType.INTEGER; }
            if (type == ClrTypes.UInt || type == ClrTypes.UIntNull) { return SQLiteDataType.INTEGER; }
            if (type == ClrTypes.Long || type == ClrTypes.LongNull) { return SQLiteDataType.INTEGER; }
            if (type == ClrTypes.ULong || type == ClrTypes.ULongNull) { return SQLiteDataType.INTEGER; }
            if (type == ClrTypes.Float || type == ClrTypes.FloatNull) { return SQLiteDataType.REAL; }
            if (type == ClrTypes.Double || type == ClrTypes.DoubleNull) { return SQLiteDataType.REAL; }
            if (type == ClrTypes.Decimal || type == ClrTypes.DecimalNull) { return SQLiteDataType.REAL; }
            if (type == ClrTypes.String) { return SQLiteDataType.TEXT; }
            if (type == ClrTypes.Guid || type == ClrTypes.GuidNull) { return SQLiteDataType.TEXT; }
            if (type == ClrTypes.DateTime || type == ClrTypes.DateTimeNull) { return SQLiteDataType.TEXT; }
            if (type == ClrTypes.DateTimeOffset || type == ClrTypes.DateTimeOffsetNull) { return SQLiteDataType.TEXT; }
            if (type == ClrTypes.ByteArray) { return SQLiteDataType.BLOB; }

            throw new ArgumentOutOfRangeException(nameof(type), $"There is no mapping between a {nameof(SQLiteDataType)} and the given type of: {type}.");
        }

        private static class ClrTypes
        {
            internal static readonly Type Bool = typeof(bool);
            internal static readonly Type BoolNull = typeof(bool?);
            internal static readonly Type Byte = typeof(byte);
            internal static readonly Type ByteNull = typeof(byte?);
            internal static readonly Type Short = typeof(short);
            internal static readonly Type ShortNull = typeof(short?);
            internal static readonly Type UShort = typeof(ushort);
            internal static readonly Type UShortNull = typeof(ushort?);
            internal static readonly Type Int = typeof(int);
            internal static readonly Type IntNull = typeof(int?);
            internal static readonly Type UInt = typeof(uint);
            internal static readonly Type UIntNull = typeof(uint?);
            internal static readonly Type Long = typeof(long);
            internal static readonly Type LongNull = typeof(long?);
            internal static readonly Type ULong = typeof(ulong);
            internal static readonly Type ULongNull = typeof(ulong?);
            internal static readonly Type Float = typeof(float);
            internal static readonly Type FloatNull = typeof(float?);
            internal static readonly Type Double = typeof(double);
            internal static readonly Type DoubleNull = typeof(double?);
            internal static readonly Type Decimal = typeof(decimal);
            internal static readonly Type DecimalNull = typeof(decimal?);
            internal static readonly Type String = typeof(string);
            internal static readonly Type Guid = typeof(Guid);
            internal static readonly Type GuidNull = typeof(Guid?);
            internal static readonly Type DateTime = typeof(DateTime);
            internal static readonly Type DateTimeNull = typeof(DateTime?);
            internal static readonly Type DateTimeOffset = typeof(DateTimeOffset);
            internal static readonly Type DateTimeOffsetNull = typeof(DateTimeOffset?);
            internal static readonly Type ByteArray = typeof(byte[]);
        }
    }
}