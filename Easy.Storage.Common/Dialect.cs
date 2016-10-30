namespace Easy.Storage.Common
{
    /// <summary>
    /// Represents the kind of the <c>SQL</c> database.
    /// </summary>
    public enum Dialect
    {
        /// <summary>
        /// Generic <c>SQL</c> dialect.
        /// </summary>
        Generic = 0,

        /// <summary>
        /// <c>SQLite</c> specific dialect.
        /// </summary>
        Sqlite,

        /// <summary>
        /// <c>SQLServer</c> specific dialect.
        /// </summary>
        SqlServer
    }
}