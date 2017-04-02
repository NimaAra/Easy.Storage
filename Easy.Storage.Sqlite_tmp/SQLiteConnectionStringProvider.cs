// ReSharper disable InconsistentNaming
namespace Easy.Storage.SQLite
{
    using System;
    using System.Data.SQLite;
    using System.IO;
    using Easy.Common;

    /// <summary>
    /// Provides helper methods for creating <c>SQLite</c> connection strings.
    /// </summary>
    public static class SQLiteConnectionStringProvider
    {
        /// <summary>
        /// Returns an in-memory <c>SQLite</c> connection-string.
        /// </summary>
        public static string GetInMemoryConnectionString()
        {
            return "Data Source=:memory:";
        }

        /// <summary>
        /// Returns a file-based <c>SQLite</c> connection-string.
        /// </summary>
        public static string GetFileConnectionString(
            FileSystemInfo file,
            bool binaryGuid = false,
            DateTimeKind dateTimeKind = DateTimeKind.Utc,
            SQLiteDateFormats dateTimeFormat = SQLiteDateFormats.UnixEpoch,
            SQLiteJournalModeEnum journalMode = SQLiteJournalModeEnum.Wal,
            SynchronizationModes syncMode = SynchronizationModes.Off,
            int pageSize = 4096,
            int cacheSize = -2000)
        {
            Ensure.NotNull(file, nameof(file));
            return GetConnectionString(
                file.FullName,
                binaryGuid,
                dateTimeKind,
                dateTimeFormat,
                journalMode,
                syncMode,
                pageSize,
                cacheSize);
        }
        
        private static string GetConnectionString(
            string dataSource, 
            bool binaryGuid, 
            DateTimeKind dateTimeKind,
            SQLiteDateFormats dateTimeFormat,
            SQLiteJournalModeEnum journalMode,
            SynchronizationModes syncMode,
            int pageSize,
            int cacheSize)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(dataSource);
            var connStr = new SQLiteConnectionStringBuilder
            {
                DataSource = dataSource,
                FailIfMissing = false,
                Pooling = false,
                BinaryGUID = binaryGuid,
                DateTimeKind = dateTimeKind,
                DateTimeFormat = dateTimeFormat,
                JournalMode = journalMode,
                SyncMode = syncMode,
                UseUTF16Encoding = false,
                ReadOnly = false,
                LegacyFormat = false,
                PageSize = pageSize,
                // reverts back to default on conn.Close has to first be set when Mode is NOT WAL & before any table is created.
                CacheSize = cacheSize // reverts back to default on conn.Close, has to first be set when Mode is NOT WAL
            }.ToString();
            return connStr;
        }
    }
}