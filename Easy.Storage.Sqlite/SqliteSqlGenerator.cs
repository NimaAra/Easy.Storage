namespace Easy.Storage.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Attributes;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.Sqlite.Models;

    /// <summary>
    /// A handy class to generate <c>SQLite</c> table scripts from a model.
    /// </summary>
    public static class SqliteSqlGenerator
    {
        private const string PrimaryKey = " PRIMARY KEY";

        private const string NotNull = " NOT NULL";

        /// <summary>
        /// Returns a <c>CREATE TABLE</c> script for the given <typeparamref name="T"/>.
        /// </summary>
        public static string Table<T>()
        {
            var table = Common.Table.Get<T>();

            var builder = new StringBuilder();
            builder.AppendLine($"CREATE TABLE IF NOT EXISTS {table.Name} (");
            builder.AppendLine($"{Formatter.Spacer}[_Entry_TimeStamp_Epoch_ms_] INTEGER DEFAULT ((julianday('now') - 2440587.5)*86400000),");

            foreach (var pair in table.PropertyToColumns)
            {
                var sqliteType = GetSqliteType(pair.Key.PropertyType).ToString();
                var isIdColumn = table.IdProperties.Contains(pair.Key);

                builder.AppendLine($"{Formatter.Spacer}{pair.Value} {sqliteType}{(isIdColumn? PrimaryKey : "")}{NotNull},");
            }

            builder.Remove(builder.Length - 3, 1);
            builder.Append(");");

            return builder.ToString();
        }

        /// <summary>
        /// Returns a <c>CREATE TABLE</c> script for the given <typeparamref name="T"/>.
        /// </summary>
        public static string FtsTable<T>(params Expression<Func<T, object>>[] selector)
        {
            var table = Common.Table.Get<T>();
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

                    string column;
                    if (propNameToColumn.TryGetValue(propName, out column))
                    {
                        columns.Add(column);
                    }
                    else
                    {
                        throw new KeyNotFoundException($"Could not find a mapping for property: {propName}. Ensure it is not marked with an {nameof(IgnoreAttribute)}.");
                    }
                }
            }

            var ftsTableName = string.Concat(table.Name, "_fts");
            var ftsColumns = string.Join(", ", columns);
            var ftsTriggerColumns = string.Join(", ", columns.Select(c => "new." + c));
            var builder = new StringBuilder();

            builder.AppendLine($"CREATE VIRTUAL TABLE IF NOT EXISTS {ftsTableName} USING FTS4 (content='{table.Name}', {ftsColumns});");
            builder.AppendLine();
            builder.AppendLine($"CREATE TRIGGER IF NOT EXISTS {table.Name}_bu BEFORE UPDATE ON {table.Name} BEGIN");
            builder.AppendLine($"{Formatter.Spacer}DELETE FROM {ftsTableName} WHERE docId = old.rowId;");
            builder.AppendLine("END;");
            builder.AppendLine();
            builder.AppendLine($"CREATE TRIGGER IF NOT EXISTS {table.Name}_bd BEFORE DELETE ON {table.Name} BEGIN");
            builder.AppendLine($"{Formatter.Spacer}DELETE FROM {ftsTableName} WHERE docId = old.rowId;");
            builder.AppendLine("END;");
            builder.AppendLine();
            builder.AppendLine($"CREATE TRIGGER IF NOT EXISTS {table.Name}_au AFTER UPDATE ON {table.Name} BEGIN");
            builder.AppendLine($"{Formatter.Spacer}INSERT INTO {ftsTableName} (docId, {ftsColumns}) VALUES (new.rowId, {ftsTriggerColumns});");
            builder.AppendLine("END;");
            builder.AppendLine();
            builder.AppendLine($"CREATE TRIGGER IF NOT EXISTS {table.Name}_ai AFTER INSERT ON {table.Name} BEGIN");
            builder.AppendLine($"{Formatter.Spacer}INSERT INTO {ftsTableName} (docId, {ftsColumns}) VALUES (new.rowId, {ftsTriggerColumns});");
            builder.Append("END;");
            return builder.ToString();
        }

        private static SqliteDataType GetSqliteType(Type type)
        {
            if (type.IsEnum) { return SqliteDataType.TEXT; }

            if (type == ClrTypes.Bool || type == ClrTypes.BoolNull) { return SqliteDataType.INTEGER; }
            if (type == ClrTypes.Byte || type == ClrTypes.ByteNull) { return SqliteDataType.INTEGER; }
            if (type == ClrTypes.Short || type == ClrTypes.ShortNull) { return SqliteDataType.INTEGER; }
            if (type == ClrTypes.UShort || type == ClrTypes.UShortNull) { return SqliteDataType.INTEGER; }
            if (type == ClrTypes.Int || type == ClrTypes.IntNull) { return SqliteDataType.INTEGER; }
            if (type == ClrTypes.UInt || type == ClrTypes.UIntNull) { return SqliteDataType.INTEGER; }
            if (type == ClrTypes.Long || type == ClrTypes.LongNull) { return SqliteDataType.INTEGER; }
            if (type == ClrTypes.ULong || type == ClrTypes.ULongNull) { return SqliteDataType.INTEGER; }
            if (type == ClrTypes.Float || type == ClrTypes.FloatNull) { return SqliteDataType.REAL; }
            if (type == ClrTypes.Double || type == ClrTypes.DoubleNull) { return SqliteDataType.REAL; }
            if (type == ClrTypes.Decimal || type == ClrTypes.DecimalNull) { return SqliteDataType.REAL; }
            if (type == ClrTypes.String) { return SqliteDataType.TEXT; }
            if (type == ClrTypes.Guid || type == ClrTypes.GuidNull) { return SqliteDataType.TEXT; }
            if (type == ClrTypes.DateTime || type == ClrTypes.DateTimeNull) { return SqliteDataType.TEXT; }
            if (type == ClrTypes.DateTimeOffset || type == ClrTypes.DateTimeOffsetNull) { return SqliteDataType.TEXT; }
            if (type == ClrTypes.ByteArray) { return SqliteDataType.BLOB; }

            throw new ArgumentOutOfRangeException(nameof(type), $"There is no mapping between a {nameof(SqliteDataType)} and the given type of: {type}.");
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