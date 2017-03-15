namespace Easy.Storage.Common
{
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Represents the type of the dialect used by the storage.
    /// </summary>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public enum DialectType
    {
        /// <summary>
        /// Generic <c>SQL</c>.
        /// </summary>
        Generic = 0,

        /// <summary>
        /// <c>SQLite</c>.
        /// </summary>
        SQLite,

        /// <summary>
        /// <c>SQLServer</c>.
        /// </summary>
        SQLServer
    }
}