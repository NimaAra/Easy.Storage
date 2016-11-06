// ReSharper disable InconsistentNaming
namespace Easy.Storage.Sqlite.Models
{
    /// <summary>
    /// Represents the object types available in a <c>SQLite</c> database.
    /// </summary>
    public enum SQLiteObjectType
    {
        /// <summary>
        /// A <c>SQLite</c> table.
        /// </summary>
        Table = 0,

        /// <summary>
        /// A <c>SQLite</c> view.
        /// </summary>
        View,

        /// <summary>
        /// A <c>SQLite</c> index.
        /// </summary>
        Index,

        /// <summary>
        /// A <c>SQLite</c> view.
        /// </summary>
        Trigger
    }
}