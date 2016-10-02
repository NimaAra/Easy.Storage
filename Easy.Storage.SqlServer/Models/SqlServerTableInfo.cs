namespace Easy.Storage.SqlServer.Models
{
    /// <summary>
    /// Represents the information relating to a <c>SQL Server</c> table.
    /// </summary>
    public sealed class SqlServerTableInfo
    {
        /// <summary>
        /// Gets the name of the database.
        /// </summary>
        public string Database { get; internal set; }

        /// <summary>
        /// Gets the name of the schema.
        /// </summary>
        public string Schema { get; internal set; }

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        public string Name { get; internal set; }

        /// <summary>
        /// Gets the columns relating to the <see cref="Name"/>.
        /// </summary>
        public SqlServerColumnInfo[] Columns { get; internal set; }
    }
}