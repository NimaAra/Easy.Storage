namespace Easy.Storage.Sqlite
{
    /// <summary>
    /// Provides <c>SQLite</c> specific <c>SQL</c> queries.
    /// </summary>
    public static class SqliteSql
    {
        /// <summary>
        /// Query all the objects from the <c>SQLite</c> master database.
        /// </summary>
        public const string Master = "SELECT * FROM SQLITE_MASTER";

        /// <summary>
        /// Query to find if a given table exists.
        /// </summary>
        public const string TableExists = "SELECT 1 FROM SQLITE_MASTER WHERE type='table' AND name=@tableName";
    }
}
