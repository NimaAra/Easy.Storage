// ReSharper disable InconsistentNaming
namespace Easy.Storage.SQLite.Models
{
    /// <summary>
    /// Represents <c>SQLite</c> data types.
    /// <remarks>
    /// Each value stored in an SQLite database (or manipulated by the database engine) has one of the following storage classes.
    /// </remarks>
    /// </summary>
    public enum SQLiteDataType
    {
        /// <summary>
        /// The value is a <c>NULL</c> value.
        /// </summary>
        NULL = 0,

        /// <summary>
        /// The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
        /// </summary>
        INTEGER,

        /// <summary>
        /// The value is a floating point value, stored as an 8-byte <c>IEEE</c> floating point number.
        /// </summary>
        REAL,

        /// <summary>
        /// The value is a text string, stored using the database encoding (<c>UTF-8</c>, <c>UTF-16BE</c> or <c>UTF-16LE</c>).
        /// </summary>
        TEXT,

        /// <summary>
        /// The value is a blob of data, stored exactly as it was input.
        /// </summary>
        BLOB
    }
}