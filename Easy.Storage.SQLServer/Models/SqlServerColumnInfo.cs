namespace Easy.Storage.SQLServer.Models
{
    /// <summary>
    /// Represents the information relating to the columns of a <c>SQL Server</c> table.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public class SQLServerColumnInfo
    {
        /// <summary>
        /// Gets the name of the column.
        /// </summary>
        public string Name { get; internal set; }

        /// <summary>
        /// Gets the type of the column.
        /// </summary>
        public SQLServerDataType Type { get; internal set; }

        /// <summary>
        /// Gets the level of precision for the column.
        /// <remarks><c>-1</c> for <c>XML</c> or large value types.</remarks>
        /// </summary>
        public short Precision { get; internal set; }

        /// <summary>
        /// Gets the ordinal position of the column.
        /// </summary>
        public int Position { get; internal set; }

        /// <summary>
        /// Gets the maximum length of the column.
        /// </summary>
        public int MaximumLength { get; internal set; }

        /// <summary>
        /// Gets the scale of the column.
        /// </summary>
        public int? Scale { get; internal set; }

        /// <summary>
        /// Gets the flag indicating whether the column can be <c>NULL</c> or not.
        /// </summary>
        public bool IsNullable { get; internal set; }

        /// <summary>
        /// Gets the name of the collation of the column.
        /// <remarks><c>NULL</c> if not a character based column.</remarks>
        /// </summary>
        public string Collation { get; internal set; }

        /// <summary>
        /// Gets the flag indicating whether the column is a <c>Primary Key</c> or not.
        /// </summary>
        public bool IsPrimaryKey { get; internal set; }
    }
}