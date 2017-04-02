namespace Easy.Storage.SQLServer.Models
{
    /// <summary>
    /// Represents the information relating to a <c>SQL Server</c> table.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public sealed class SQLServerTableInfo
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
        public SQLServerColumnInfo[] Columns { get; internal set; }
    }
}