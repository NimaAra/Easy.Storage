namespace Easy.Storage.Common
{
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Represents the kind of the <c>SQL</c> database.
    /// </summary>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public enum Dialect
    {
        /// <summary>
        /// Generic <c>SQL</c> dialect.
        /// </summary>
        Generic = 0,

        /// <summary>
        /// <c>SQLite</c> specific dialect.
        /// </summary>
        SQLite,

        /// <summary>
        /// <c>SQLServer</c> specific dialect.
        /// </summary>
        SQLServer
    }
}