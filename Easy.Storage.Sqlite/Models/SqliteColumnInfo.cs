namespace Easy.Storage.Sqlite.Models
{
    /// <summary>
    /// Represents the information relating to the columns of a <c>SQLite</c> table.
    /// </summary>
    public sealed class SqliteColumnInfo
    {
        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        public string TableName { get; internal set; }

        /// <summary>
        /// Gets the Id of the column.
        /// </summary>
        public long ColumnId { get; internal set; }

        /// <summary>
        /// Gets the name of the column.
        /// </summary>
        public string ColumnName { get; internal set; }

        /// <summary>
        /// Gets the type of the column.
        /// </summary>
        public SqliteDataType ColumnTpe { get; internal set; }

        /// <summary>
        /// Gets the flag indicating whether the column can be <c>NULL</c> or not.
        /// </summary>
        public bool NotNull { get; internal set; }

        /// <summary>
        /// Gets the default value of the column.
        /// </summary>
        public string DefaultValue { get; internal set; }

        /// <summary>
        /// Gets the flag indicating whether the column is a <c>Primary Key</c> or not.
        /// </summary>
        public bool IsPrimaryKey { get; internal set; }
    }
}