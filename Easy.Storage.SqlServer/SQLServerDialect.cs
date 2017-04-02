namespace Easy.Storage.SQLServer
{
    using System;
    using System.IO;
    using System.Linq;
    using Easy.Common;
    using Easy.Common.Extensions;
    using Easy.Storage.Common;

    /// <summary>
    /// Represents the <see cref="Dialect"/> used by <c>Microsoft SQL Server</c>.
    /// </summary>
    public sealed class SQLServerDialect : Dialect
    {
        private const string InsertedRowId = "SELECT Id FROM @InsertedRows";

        static SQLServerDialect() { }
        private SQLServerDialect() : base(DialectType.SQLServer) { }

        /// <summary>
        /// Gets a single instance of the <see cref="SQLServerDialect"/>.
        /// </summary>
        public static SQLServerDialect Instance { get; } = new SQLServerDialect();

        internal override string GetInsertQuery(Table table, bool includeIdentity)
        {
            var columnsAndProps = GetColumnsAndProperties(table, includeIdentity);
            var insertSeg = $"INSERT INTO {table.Name}{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columnsAndProps.Key}{Formatter.NewLine})";
            var valuesSeg = $"{Formatter.NewLine}VALUES{Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{columnsAndProps.Value}{Formatter.NewLine});";
            var idColumnName = table.PropertyToColumns[table.IdentityColumn];
            var outputClause = $"OUTPUT Inserted.{idColumnName} INTO @InsertedRows";
            return $"{GetDeclarationSegment(table)}{Formatter.NewLine}{insertSeg} {outputClause}{valuesSeg}{Formatter.NewLine}{InsertedRowId};";
        }

        internal override string GetPartialInsertQuery<T>(Table table, object item)
        {
            var builder = StringBuilderCache.Acquire();
            builder.Append(GetDeclarationSegment(table)).AppendLine()
                .Append("INSERT INTO ").Append(table.Name).AppendLine().Append('(').AppendLine();

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

            var idColumnName = table.PropertyToColumns[table.IdentityColumn];
            var outputClause = $"OUTPUT Inserted.{idColumnName} INTO @InsertedRows";

            builder.AppendLine().Append(") ").Append(outputClause).AppendLine()
                .Append("VALUES").AppendLine().Append('(').AppendLine();

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

        private static string GetDeclarationSegment(Table table)
        {
            var idColumnType = table.IdentityColumn.PropertyType;

            string sqlServerDataType;
            if (idColumnType == typeof(string))
            {
                sqlServerDataType = "NVARCHAR(MAX)";
            }
            else if (idColumnType == typeof(byte))
            {
                sqlServerDataType = "TINYINT";
            }
            else if (idColumnType == typeof(short))
            {
                sqlServerDataType = "SMALLINT";
            }
            else if (idColumnType == typeof(int))
            {
                sqlServerDataType = "INT";
            }
            else if (idColumnType == typeof(long))
            {
                sqlServerDataType = "BIGINT";
            }
            else if (idColumnType == typeof(Guid))
            {
                sqlServerDataType = "UNIQUEIDENTIFIER";
            }
            else if (idColumnType == typeof(bool))
            {
                sqlServerDataType = "BIT";
            }
            else if (idColumnType == typeof(byte[]))
            {
                sqlServerDataType = "VARBINARY";
            }
            else
            {
                throw new NotSupportedException($"The requested identity column of: {table.IdentityColumn.Name} with the type: {idColumnType.Name} is not supported.");
            }

            return $"DECLARE @InsertedRows AS TABLE (Id {sqlServerDataType});";
        }
    }
}