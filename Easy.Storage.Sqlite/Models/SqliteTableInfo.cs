namespace Easy.Storage.Sqlite.Models
{
    /// <summary>
    /// Represents the information relating to a <c>SQLite</c> table.
    /// </summary>
    public sealed class SqliteTableInfo
    {
        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        public string TableName { get; internal set; }

        /// <summary>
        /// Gets the <c>SQL</c> query used to define the object.
        /// </summary>
        public string Sql { get; internal set; }

        /// <summary>
        /// Gets the columns relating to the <see cref="TableName"/>.
        /// </summary>
        public SqliteColumnInfo[] Columns { get; internal set; }
    }
}