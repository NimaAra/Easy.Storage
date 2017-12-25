namespace Easy.Storage.Common
{
    using Easy.Storage.Common.Filter;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Easy.Common;
    using Easy.Common.Extensions;

    /// <summary>
    /// An abstraction for specifying the set of commands and languages
    /// used by the back-end storage provider which is then utilized by this library.
    /// </summary>
    public abstract class Dialect
    {
        /// <summary>
        /// Creates an instance of the <see cref="Dialect"/>.
        /// </summary>
        protected Dialect(DialectType type)
        {
            Type = type;
        }

        /// <summary>
        /// Gets the type of the dialect.
        /// </summary>
        public DialectType Type { get; }

        internal abstract string GetPartialInsertQuery<T>(Table table, object item);
        
        internal virtual string GetSelectQuery(Table table)
        {
            var propNames = table.PropertyToColumns.Keys.Select(p => p.Name).ToArray();
            var colNames = table.PropertyToColumns.Values.ToArray();
            var colsAsPropNameAlias = string.Join(Formatter.ColumnSeparator, colNames.Zip(propNames, (col, prop) => $"{table.Name}.{col} AS '{prop}'"));
            return $"SELECT{Formatter.NewLine}{Formatter.Spacer}{colsAsPropNameAlias}{Formatter.NewLine}FROM {table.Name}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";
        }

        internal virtual string GetDeleteQuery(Table table) => $"DELETE FROM {table.Name}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";

        internal virtual string GetInsertQuery(Table table, bool includeIdentity)
        {
            var columnsAndProps = GetColumnsAndProperties(table, includeIdentity);
            var insertSeg = $"INSERT INTO {table.Name}{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columnsAndProps.Key}{Formatter.NewLine})";
            var valuesSeg = $"{Formatter.NewLine}VALUES{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columnsAndProps.Value}{Formatter.NewLine});";
            return insertSeg + valuesSeg;
        }

        internal virtual string GetUpdateQuery(Table table, bool includeIdentity)
        {
            if (includeIdentity)
            {
                var propNames = table.PropertyToColumns.Keys.Select(p => p.Name).ToArray();
                var colNames = table.PropertyToColumns.Values.ToArray();
                var allColsEqualProp = string.Join(Formatter.ColumnSeparator, colNames.Zip(propNames, (col, propName) => $"{col} = @{propName}"));
                return $"UPDATE {table.Name} SET{Formatter.NewLine}{Formatter.Spacer}{allColsEqualProp}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}1 = 1;";
            }

            var propToColsMinusIdentity = table.PropertyToColumns.Where(p => p.Key != table.IdentityColumn).ToArray();
            var colNamesMinusIdentity = propToColsMinusIdentity.Select(kv => kv.Value).ToArray();
            var propNamesMinusIdentity = propToColsMinusIdentity.Select(kv => kv.Key.Name).ToArray();
            var colEqualPropMinusIdentity = string.Join(Formatter.ColumnSeparator, colNamesMinusIdentity.Zip(propNamesMinusIdentity, (col, propName) => $"{col} = @{propName}"));
            return $"UPDATE {table.Name} SET{Formatter.NewLine}{Formatter.Spacer}{colEqualPropMinusIdentity}{Formatter.NewLine}WHERE{Formatter.NewLine}{Formatter.Spacer}{table.PropertyToColumns[table.IdentityColumn]} = @{table.IdentityColumn.Name};";
        }

        internal string GetPartialUpdateQuery<T>(Table table, object item, Filter<T> filter)
        {
            var builder = StringBuilderCache.Acquire();
            builder.Append("UPDATE ").Append(table.Name).Append(" SET").AppendLine();

            var propNames = item.GetPropertyNames(true, false);

            Ensure.That<InvalidDataException>(propNames.Any(), "Unable to find any properties in: " + nameof(item));

            foreach (var pName in propNames)
            {
                if (table.IgnoredProperties.Contains(pName)) { continue; }

                Ensure.That<InvalidDataException>(
                    table.PropertyNamesToColumns.TryGetValue(pName, out string colName),
                    $"Property: '{pName}' does not exist on the model: '{typeof(T).Name}'.");

                builder.Append(Formatter.Spacer)
                    .Append(colName).Append(" = @").Append(pName)
                    .Append(Formatter.ColumnSeparatorNoSpace);
            }

            builder.Remove(
                builder.Length - Formatter.ColumnSeparatorNoSpace.Length,
                Formatter.ColumnSeparatorNoSpace.Length);

            builder.Append(Formatter.NewLine).Append("WHERE 1=1");

            return filter.GetSQL(StringBuilderCache.GetStringAndRelease(builder));
        }

        /// <summary>
        /// Gets the columns and their corresponding property names.
        /// </summary>
        protected static KeyValuePair<string, string> GetColumnsAndProperties(Table table, bool includeIdentity)
        {
            var allColNames = table.PropertyToColumns.Select(kv => kv.Value).ToArray();
            var allPropNames = table.PropertyToColumns.Select(kv => kv.Key.Name).ToArray();

            string columns, properties;
            if (includeIdentity)
            {
                columns = string.Join(Formatter.ColumnSeparator, allColNames);
                properties = string.Join(Formatter.ColumnSeparator, allPropNames.Select(x => "@" + x));
            } else
            {
                var propToColsMinusIdentity = table.PropertyToColumns.Where(p => p.Key != table.IdentityColumn).ToArray();
                var colNamesMinusIdentity = propToColsMinusIdentity.Select(kv => kv.Value).ToArray();
                var propNamesMinusIdentity = propToColsMinusIdentity.Select(kv => kv.Key.Name).ToArray();

                columns = string.Join(Formatter.ColumnSeparator, colNamesMinusIdentity);
                properties = string.Join(Formatter.ColumnSeparator, propNamesMinusIdentity.Select(x => "@" + x));
            }

            return new KeyValuePair<string, string>(columns, properties);
        }
    }
}