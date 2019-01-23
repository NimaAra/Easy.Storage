namespace Easy.Storage.SQLite
{
    /// <summary>
    /// Provides <c>SQLite</c> specific <c>SQL</c> queries.
    /// </summary>
    public static class SQLiteSQL
    {
        /// <summary>
        /// Query all the objects from the <c>SQLite</c> master database.
        /// </summary>
        public const string Master = "SELECT * FROM SQLITE_MASTER";

        /// <summary>
        /// Query all the attached databases.
        /// </summary>
        public const string AttachedDatabases = "PRAGMA database_list";

        /// <summary>
        /// Rebuilds the database file, repacking it into a minimal amount of disk space.
        /// </summary>
        public const string Vacuum = "VACUUM";

        /// <summary>
        /// Query to find if a given table exists.
        /// </summary>
        public const string TableExists = "SELECT 1 FROM SQLITE_MASTER WHERE type='table' AND name=@tableName";
    }
}