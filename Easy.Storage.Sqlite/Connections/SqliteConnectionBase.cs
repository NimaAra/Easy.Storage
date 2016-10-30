namespace Easy.Storage.Sqlite.Connections
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Data.SQLite;
    using Easy.Common;

    /// <summary>
    /// An abstraction over <see cref="SQLiteConnection"/>.
    /// </summary>
    [System.ComponentModel.DesignerCategory("")]
    public abstract class SqliteConnectionBase : DbConnection, IDisposable
    {
        /// <summary>
        /// The underlying <c>SQLite</c> connection.
        /// </summary>
        protected readonly SQLiteConnection Connection;

        /// <summary>
        /// Creates an instance of the <see cref="SqliteConnectionBase"/>.
        /// </summary>
        /// <param name="connectionString">A valid <c>SQLite</c> connection-string.</param>
        protected SqliteConnectionBase(string connectionString)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(connectionString, "Connection string cannot be null, empty or whitespace.");
            Connection = new SQLiteConnection(connectionString);
        }

        /// <summary>
        /// Gets the connection-string containing the parameters for the connection.
        /// </summary>
        public sealed override string ConnectionString
        {
            get { return Connection.ConnectionString; }
            set { /* ignored */ }
        }

        /// <summary>
        /// Gets the time in seconds to wait while establishing a connection 
        /// before terminating the attempt and generating an error.
        /// </summary>
        public override int ConnectionTimeout => Connection.ConnectionTimeout;

        /// <summary>
        /// Gets the version of the underlying database engine.
        /// </summary>
        public override string ServerVersion => Connection.ServerVersion;

        /// <summary>
        /// Gets the state of the <see cref="SqliteConnectionBase"/>.
        /// </summary>
        public override ConnectionState State => Connection.State;

        /// <summary>
        /// Gets the database name.
        /// <remarks>Returns the string 'main'</remarks>
        /// </summary>
        public override string Database => Connection.Database;

        /// <summary>
        /// Gets the data source file name without extension or path. 
        /// </summary>
        public override string DataSource => Connection.DataSource;

        /// <summary>
        /// Creates a new <see cref="SQLiteTransaction"/> if one isn't already active on the connection.
        /// <remarks>
        /// Unspecified will use the default isolation level specified in the connection string. 
        /// If no isolation level is specified in the connection string, Serializable is used. 
        /// Serializable transactions are the default. In this mode, the engine gets an immediate 
        /// lock on the database, and no other threads may begin a transaction. Other threads may 
        /// read from the database, but not write. With a ReadCommitted isolation level, locks are 
        /// deferred and elevated as needed. It is possible for multiple threads to start a transaction 
        /// in ReadCommitted mode, but if a thread attempts to commit a transaction while another thread 
        /// has a ReadCommitted lock, it may timeout or cause a deadlock on both threads until both threads'
        /// CommandTimeout's are reached. 
        /// </remarks>
        /// </summary>
        /// <param name="isolationLevel">Supported isolation levels are Serializable, ReadCommitted and Unspecified.</param>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return Connection.BeginTransaction(isolationLevel);
        }

        /// <summary>
        /// When the database connection is closed, all commands linked to this connection are automatically reset.
        /// </summary>
        public override void Close()
        {
            Connection.Close();
        }

        /// <summary>
        /// This method is not implemented; however, the <c>Changed</c> event will still be raised. 
        /// </summary>
        public override void ChangeDatabase(string databaseName)
        {
            Connection.ChangeDatabase(databaseName);
        }

        /// <summary>
        /// Create a new <see cref="SQLiteCommand"/> and associate it with this connection. 
        /// </summary>
        protected override DbCommand CreateDbCommand()
        {
            return Connection.CreateCommand();
        }

        /// <summary>
        /// Opens the connection using the parameters found in the <see cref="ConnectionString"/>. 
        /// </summary>
        public override void Open()
        {
            Connection.Open();
        }

        /// <summary>
        /// Disposes and finalizes the connection, if applicable.
        /// </summary>
        public new abstract void Dispose();
    }
}