namespace Easy.Storage.Sqlite.Connections
{
    using System.Data.SQLite;
    using Easy.Common;

    /// <summary>
    /// A wrapper around the <see cref="SQLiteConnection"/> to allow reuse of in-memory connections.
    /// </summary>
    public sealed class SqliteInMemoryConnection : SqliteConnectionBase
    {
        /// <summary>
        /// Creates an instance of the <see cref="SqliteInMemoryConnection"/>.
        /// </summary>
        public SqliteInMemoryConnection() : base(SqliteConnectionStringBuilder.GetInMemoryConnectionString()) {}

        /// <summary>
        /// Creates an instance of the <see cref="SqliteInMemoryConnection"/>.
        /// </summary>
        /// <param name="connectionString">A valid <c>SQLite</c> connection-string.</param>
        public SqliteInMemoryConnection(string connectionString) : base(connectionString)
        {
            Ensure.That(SqliteHelper.IsInMemoryConnection(connectionString), 
                "Not a valid SQLite memory connection-string.");
        }

        /// <summary>
        /// Does not close the connection. Dispose the connection for the connection to close.
        /// </summary>
        public override void Close()
        {
            // do nothing.
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