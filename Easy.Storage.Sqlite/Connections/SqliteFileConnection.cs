// ReSharper disable InconsistentNaming
namespace Easy.Storage.Sqlite.Connections
{
    using System.Data.SQLite;
    using System.IO;
    using Easy.Common;

    /// <summary>
    /// A wrapper around the <see cref="SQLiteConnection"/> for file-based connections.
    /// </summary>
    public sealed class SQLiteFileConnection : SQLiteConnectionBase
    {
        /// <summary>
        /// Creates an instance of the <see cref="SQLiteInMemoryConnection"/>.
        /// </summary>
        public SQLiteFileConnection(FileSystemInfo fileInfo) 
            : base(SQLiteConnectionStringProvider.GetFileConnectionString(fileInfo)) {}

        /// <summary>
        /// Creates an instance of the <see cref="SQLiteInMemoryConnection"/>.
        /// </summary>
        /// <param name="connectionString">A valid <c>SQLite</c> connection-string.</param>
        public SQLiteFileConnection(string connectionString) : base(connectionString)
        {
            Ensure.That(!SQLiteHelper.IsInMemoryConnection(connectionString),
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