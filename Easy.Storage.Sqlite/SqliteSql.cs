namespace Easy.Storage.Sqlite
{
    /// <summary>
    /// Provides <c>SQLite</c> specific <c>SQL</c> queries.
    /// </summary>
    public static class SqliteSql
    {
        /// <summary>
        /// Queries all the objects from the <c>SQLite</c> master database.
        /// </summary>
        public const string Master = "SELECT * FROM SQLITE_MASTER";

        /// <summary>
        /// Queries to find if a given table exists.
        /// </summary>
        public const string TableExists = "SELECT COUNT(name) FROM SQLITE_MASTER WHERE type='table' AND name=@tableName";
    }
}
