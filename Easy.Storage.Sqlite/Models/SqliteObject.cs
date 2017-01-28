// ReSharper disable UnassignedGetOnlyAutoProperty
// ReSharper disable InconsistentNaming
namespace Easy.Storage.SQLite.Models
{
    /// <summary>
    /// Represents objects defined in a <c>SQLite</c> database.
    /// </summary>
    // ReSharper disable once ClassNeverInstantiated.Global
    public sealed class SQLiteObject
    {
        /// <summary>
        /// Gets the type of the object.
        /// </summary>
        public SQLiteObjectType Type { get; }

        /// <summary>
        /// Gets the name of the object.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the <c>SQL</c> query used to define the object.
        /// </summary>
        public string SQL { get; }
    }
}