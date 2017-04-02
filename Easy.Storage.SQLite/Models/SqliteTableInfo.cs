// ReSharper disable InconsistentNaming
namespace Easy.Storage.SQLite.Models
{
    /// <summary>
    /// Represents the information relating to a <c>SQLite</c> table.
    /// </summary>
    public sealed class SQLiteTableInfo
    {
        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        public string TableName { get; internal set; }

        /// <summary>
        /// Gets the <c>SQL</c> query used to define the object.
        /// </summary>
        public string SQL { get; internal set; }

        /// <summary>
        /// Gets the columns relating to the <see cref="TableName"/>.
        /// </summary>
        public SQLiteColumnInfo[] Columns { get; internal set; }
    }
}