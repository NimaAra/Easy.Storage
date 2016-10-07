namespace Easy.Storage.Sqlite
{
    using System;
    using System.Data.SQLite;
    using System.IO;
    using Easy.Common;

    /// <summary>
    /// Provides helper methods for creating <see cref="SqliteConnectionWrapper"/>.
    /// </summary>
    public static class SqliteConnectionBuilder
    {
        /// <summary>
        /// Returns an in-memory <see cref="SqliteConnectionWrapper"/>.
        /// </summary>
        public static SqliteConnectionWrapper GetInMemoryConnectionWrapper()
        {
            return new SqliteConnectionWrapper(new SQLiteConnection(GetInMemoryConnectionString()));
        }

        /// <summary>
        /// Returns an file-based <see cref="SqliteConnectionWrapper"/>.
        /// </summary>
        public static SqliteConnectionWrapper GetFileConnectionWrapper(FileInfo file, TimeSpan? rollEvery = null, bool fromStartOfToday = false)
        {
            Ensure.NotNull(file, nameof(file));
            return new SqliteConnectionWrapper(new SQLiteConnection(GetConnectionString(file.FullName)), rollEvery, fromStartOfToday);
        }

        /// <summary>
        /// Returns an in-memory <see cref="SqliteConnectionWrapper"/>.
        /// </summary>
        public static string GetInMemoryConnectionString()
        {
            return GetConnectionString(":memory:");
        }

        /// <summary>
        /// Returns an file-based <see cref="SqliteConnectionWrapper"/>.
        /// </summary>
        public static string GetFileConnectionString(FileInfo file)
        {
            Ensure.NotNull(file, nameof(file));
            return GetConnectionString(file.FullName);
        }
        
        private static string GetConnectionString(string dataSource)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(dataSource);
            var connStr = new SQLiteConnectionStringBuilder
            {
                DataSource = dataSource,
                FailIfMissing = false,
                Pooling = false,
                BinaryGUID = true,
                DateTimeKind = DateTimeKind.Utc,
                DateTimeFormat = SQLiteDateFormats.UnixEpoch,
                JournalMode = SQLiteJournalModeEnum.Wal,
                SyncMode = SynchronizationModes.Off,
                UseUTF16Encoding = false,
                ReadOnly = false,
                LegacyFormat = false,
                PageSize = 4096,
                // reverts back to default on conn.Close has to first be set when Mode is NOT WAL & before any table is created.
                CacheSize = -2000 // reverts back to default on conn.Close, has to first be set when Mode is NOT WAL
            }.ToString();
            return connStr;
        }
    }
}