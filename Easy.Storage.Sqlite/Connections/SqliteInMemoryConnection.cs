// ReSharper disable InconsistentNaming
namespace Easy.Storage.SQLite.Connections
{
    using System.Data.SQLite;
    using Easy.Common;

    /// <summary>
    /// A wrapper around the <see cref="SQLiteConnection"/> to allow reuse of in-memory connections.
    /// </summary>
    public sealed class SQLiteInMemoryConnection : SQLiteConnectionBase
    {
        /// <summary>
        /// Creates an instance of the <see cref="SQLiteInMemoryConnection"/>.
        /// </summary>
        public SQLiteInMemoryConnection() : base(SQLiteConnectionStringProvider.GetInMemoryConnectionString()) {}

        /// <summary>
        /// Creates an instance of the <see cref="SQLiteInMemoryConnection"/>.
        /// </summary>
        /// <param name="connectionString">A valid <c>SQLite</c> connection-string.</param>
        public SQLiteInMemoryConnection(string connectionString) : base(connectionString)
        {
            Ensure.That(SQLiteHelper.IsInMemoryConnection(connectionString), 
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