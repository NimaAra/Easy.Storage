namespace Easy.Storage.SQLite
{
    using Easy.Storage.Common;

    /// <summary>
    /// Represents the <see cref="Dialect"/> used by <c>SQLite</c>.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public sealed class SQLiteDialect : Dialect
    {
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
            return $"{insertSeg}{valuesSeg}{Formatter.NewLine}SELECT last_insert_rowid() AS Id;";
        }
    }
}