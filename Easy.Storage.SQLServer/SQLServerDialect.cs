namespace Easy.Storage.SQLServer
{
    using System;
    using Easy.Storage.Common;

    /// <summary>
    /// Represents the <see cref="Dialect"/> used by <c>Microsoft SQL Server</c>.
    /// </summary>
    public sealed class SQLServerDialect : Dialect
    {
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
            return $"{GetDeclarationSegment(table)}{Formatter.NewLine}{insertSeg} {outputClause}{valuesSeg}{Formatter.NewLine}SELECT Id FROM @InsertedRows;";
        }

        private static string GetDeclarationSegment(Table table)
        {
            var idColumnType = table.IdentityColumn.PropertyType;

            string sqlServerDataType;
            if (idColumnType == typeof(string))
            {
                sqlServerDataType = "NVARCHAR(MAX)";
            } else if (idColumnType == typeof(byte))
            {
                sqlServerDataType = "TINYINT";
            } else if (idColumnType == typeof(short))
            {
                sqlServerDataType = "SMALLINT";
            } else if (idColumnType == typeof(int))
            {
                sqlServerDataType = "INT";
            } else if (idColumnType == typeof(long))
            {
                sqlServerDataType = "BIGINT";
            } else if (idColumnType == typeof(Guid))
            {
                sqlServerDataType = "UNIQUEIDENTIFIER";
            } else if (idColumnType == typeof(bool))
            {
                sqlServerDataType = "BIT";
            } else if (idColumnType == typeof(byte[]))
            {
                sqlServerDataType = "VARBINARY";
            } else
            {
                throw new NotSupportedException($"The requested identity column of: {table.IdentityColumn.Name} with the type: {idColumnType.Name} is not supported.");
            }

            return $"DECLARE @InsertedRows AS TABLE (Id {sqlServerDataType});";
        }
    }
}