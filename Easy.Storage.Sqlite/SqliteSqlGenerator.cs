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
            var tableName = table.Name.Replace("[", string.Empty).Replace("]", string.Empty);

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

            var ftsTableName = string.Concat(tableName, "_fts");
            var ftsColumns = string.Join(", ", columns);
            var ftsTriggerColumns = string.Join(", ", columns.Select(c => "new." + c));
            var builder = new StringBuilder();

            builder.AppendLine($"CREATE VIRTUAL TABLE IF NOT EXISTS {ftsTableName} USING FTS4 (content='{tableName}', {ftsColumns});");
            builder.AppendLine();
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
            return builder.ToString();
        }

        private static SqliteDataType GetSqliteType(Type clrType)
        {
            if (clrType.IsEnum) { return SqliteDataType.TEXT; }

            if (clrType == ClrTypes.Bool) { return SqliteDataType.INTEGER; }
            if (clrType == ClrTypes.Byte) { return SqliteDataType.INTEGER; }
            if (clrType == ClrTypes.Short) { return SqliteDataType.INTEGER; }
            if (clrType == ClrTypes.UShort) { return SqliteDataType.INTEGER; }
            if (clrType == ClrTypes.Int) { return SqliteDataType.INTEGER; }
            if (clrType == ClrTypes.UInt) { return SqliteDataType.INTEGER; }
            if (clrType == ClrTypes.Long) { return SqliteDataType.INTEGER; }
            if (clrType == ClrTypes.ULong) { return SqliteDataType.INTEGER; }
            if (clrType == ClrTypes.Float) { return SqliteDataType.REAL; }
            if (clrType == ClrTypes.Double) { return SqliteDataType.REAL; }
            if (clrType == ClrTypes.Decimal) { return SqliteDataType.REAL; }
            if (clrType == ClrTypes.String) { return SqliteDataType.TEXT; }
            if (clrType == ClrTypes.Guid) { return SqliteDataType.TEXT; }
            if (clrType == ClrTypes.DateTime) { return SqliteDataType.TEXT; }
            if (clrType == ClrTypes.DateTimeOffset) { return SqliteDataType.TEXT; }
            if (clrType == ClrTypes.ByteArray) { return SqliteDataType.BLOB; }

            throw new ArgumentOutOfRangeException(nameof(clrType), $"There is no mapping between a {nameof(SqliteDataType)} and the CLR type.");
        }

        private static class ClrTypes
        {
            internal static readonly Type Bool = typeof(bool);
            internal static readonly Type Byte = typeof(byte);
            internal static readonly Type Short = typeof(short);
            internal static readonly Type UShort = typeof(ushort);
            internal static readonly Type Int = typeof(int);
            internal static readonly Type UInt = typeof(uint);
            internal static readonly Type Long = typeof(long);
            internal static readonly Type ULong = typeof(ulong);
            internal static readonly Type Float = typeof(float);
            internal static readonly Type Double = typeof(double);
            internal static readonly Type Decimal = typeof(decimal);
            internal static readonly Type String = typeof(string);
            internal static readonly Type Guid = typeof(Guid);
            internal static readonly Type DateTime = typeof(DateTime);
            internal static readonly Type DateTimeOffset = typeof(DateTimeOffset);
            internal static readonly Type ByteArray = typeof(byte[]);
        }
    }
}