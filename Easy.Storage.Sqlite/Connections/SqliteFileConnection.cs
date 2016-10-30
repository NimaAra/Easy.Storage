namespace Easy.Storage.Sqlite.Connections
{
    using System.Data.SQLite;
    using System.IO;
    using Easy.Common;

    /// <summary>
    /// A wrapper around the <see cref="SQLiteConnection"/> for file-based connections.
    /// </summary>
    public sealed class SqliteFileConnection : SqliteConnectionBase
    {
        /// <summary>
        /// Creates an instance of the <see cref="SqliteInMemoryConnection"/>.
        /// </summary>
        public SqliteFileConnection(FileSystemInfo fileInfo) 
            : base(SqliteConnectionStringBuilder.GetFileConnectionString(fileInfo)) {}

        /// <summary>
        /// Creates an instance of the <see cref="SqliteInMemoryConnection"/>.
        /// </summary>
        /// <param name="connectionString">A valid <c>SQLite</c> connection-string.</param>
        public SqliteFileConnection(string connectionString) : base(connectionString)
        {
            Ensure.That(!SqliteHelper.IsInMemoryConnection(connectionString),
                "Cannot be a SQLite memory connection-string.");
        }

        /// <summary>
        /// Disposes and finalizes the connection, if applicable.
        /// </summary>
        public override void Dispose()
        {
            Connection.Dispose();
        }
    }
}