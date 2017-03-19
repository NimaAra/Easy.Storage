namespace Easy.Storage.SQLite
{
    using System.IO;
    using System.Linq;
    using Easy.Common;
    using Easy.Common.Extensions;
    using Easy.Storage.Common;

    /// <summary>
    /// Represents the <see cref="Dialect"/> used by <c>SQLite</c>.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public sealed class SQLiteDialect : Dialect
    {
        private const string InsertedRowId = "SELECT last_insert_rowid() AS Id";

        static SQLiteDialect() { }
        private SQLiteDialect() : base(DialectType.SQLite) { }

        /// <summary>
        /// Gets a single instance of the <see cref="SQLiteDialect"/>.
        /// </summary>
        public static SQLiteDialect Instance { get; } = new SQLiteDialect();

        internal override string GetInsertQuery(Table table, bool includeIdentity)
        {
            var columnsAndProps = GetColumnsAndProperties(table, includeIdentity);
            var insertSeg = $"INSERT INTO {table.Name}{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columnsAndProps.Key}{Formatter.NewLine})";
            var valuesSeg = $"{Formatter.NewLine}VALUES{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columnsAndProps.Value}{Formatter.NewLine});";

            return $"{insertSeg}{valuesSeg}{Formatter.NewLine}{InsertedRowId};";
        }

        internal override string GetPartialInsertQuery<T>(Table table, object item)
        {
            var builder = StringBuilderCache.Acquire();
            builder.Append("INSERT INTO ").Append(table.Name).AppendLine().Append('(').AppendLine();

            var propNames = item.GetPropertyNames(true, false);

            Ensure.That<InvalidDataException>(propNames.Any(), "Unable to find any properties in: " + nameof(item));

            // 1st pass to compose columns
            foreach (var pName in propNames)
            {
                if (table.IgnoredProperties.Contains(pName)) { continue; }

                Ensure.That<InvalidDataException>(
                    table.PropertyNamesToColumns.TryGetValue(pName, out string colName),
                    $"Property: '{pName}' does not exist on the model: '{typeof(T).Name}'.");

                builder.Append(Formatter.Spacer).Append(colName).Append(Formatter.ColumnSeparatorNoSpace);
            }

            builder.Remove(
                builder.Length - Formatter.ColumnSeparatorNoSpace.Length,
                Formatter.ColumnSeparatorNoSpace.Length);

            builder.AppendLine().Append(") VALUES (").AppendLine();

            // 2nd pass to compose values
            foreach (var pName in propNames)
            {
                if (table.IgnoredProperties.Contains(pName)) { continue; }
                builder.Append(Formatter.Spacer).Append('@').Append(pName).Append(Formatter.ColumnSeparatorNoSpace);
            }

            builder.Remove(
                builder.Length - Formatter.ColumnSeparatorNoSpace.Length,
                Formatter.ColumnSeparatorNoSpace.Length);

            builder.AppendLine().Append(");").AppendLine().Append(InsertedRowId).Append(';');
            return StringBuilderCache.GetStringAndRelease(builder);
        }
    }
}