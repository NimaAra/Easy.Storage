namespace Easy.Storage.Sqlite
{
    using Easy.Common.Extensions;
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Data.SQLite;
    using System.IO;
    using System.Text;
    using Easy.Common;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.Sqlite.Models;

    /// <summary>
    /// A wrapper around the <see cref="SQLiteConnection"/> to allow reuse of in-memory connections.
    /// </summary>
    public sealed class SqliteConnectionWrapper : DbConnection
    {
        private readonly object _locker = new object();
        private readonly bool _fromStartOfToday;
        private SQLiteConnection _connection;
        private volatile bool _isDisposed;
        private DateTime _connectionLastCreationTime;

        /// <summary>
        /// Gets the flag indicating whether the underlying connection is <c>In-Memory</c> or not.
        /// </summary>
        public bool IsInMemory { get; }

        /// <summary>
        /// Gets the version of the underlying database engine.
        /// </summary>
        public override string ServerVersion => _connection.ServerVersion;

        /// <summary>
        /// Gets the state of the <see cref="SqliteConnectionWrapper"/>.
        /// </summary>
        public override ConnectionState State => _isDisposed ? ConnectionState.Closed : _connection.State;

        /// <summary>
        /// Creates an instance of the <see cref="SqliteConnectionWrapper"/>.
        /// </summary>
        /// <param name="connectionString">A valid <c>SQLite</c> connection-string.</param>
        /// <param name="rollEvery">The <see cref="TimeSpan"/> at which the connection database file should be rolled.</param>
        /// <param name="fromStartOfToday">The flag indicating whether rolling should start from the start of today or from now.</param>
        public SqliteConnectionWrapper(string connectionString, TimeSpan? rollEvery = null, bool fromStartOfToday = false)
            : this(new SQLiteConnection(connectionString), rollEvery, fromStartOfToday) {}

        /// <summary>
        /// Creates an instance of the <see cref="SqliteConnectionWrapper"/>.
        /// </summary>
        /// <param name="sqliteConnection">A <c>SQLite</c> connection.</param>
        /// <param name="rollEvery">The <see cref="TimeSpan"/> at which the connection database file should be rolled.</param>
        /// <param name="fromStartOfToday">The flag indicating whether rolling should start from the start of today or from now.</param>
        public SqliteConnectionWrapper(SQLiteConnection sqliteConnection, TimeSpan? rollEvery = null, bool fromStartOfToday = false)
        {
            _fromStartOfToday = fromStartOfToday;
            _connection = Ensure.NotNull(sqliteConnection, nameof(sqliteConnection));
            IsInMemory = _connection.ConnectionString.Contains(":memory:", StringComparison.OrdinalIgnoreCase);
            RollEvery = rollEvery?? TimeSpan.MaxValue;

            var now = DateTime.Now;
            _connectionLastCreationTime = fromStartOfToday ? now.Date : now;
        }

        /// <summary>
        /// Gets the number of times the connection has rolled.
        /// </summary>
        public uint RollCount { get; private set; }

        /// <summary>
        /// Gets the <see cref="TimeSpan"/> by which the connection database file should be rolled.
        /// </summary>
        public TimeSpan RollEvery { get; }

        /// <summary>
        /// Gets the connection-string containing the parameters for the connection.
        /// </summary>
        public override string ConnectionString
        {
            get { return _connection.ConnectionString; }
            set { /* ignored */ }
        }

        /// <summary>
        /// Gets the time in seconds to wait while establishing a connection before terminating the attempt and generating an error.
        /// </summary>
        public override int ConnectionTimeout => _connection.ConnectionTimeout;

        /// <summary>
        /// Gets the database name.
        /// <remarks>Returns the string 'main'</remarks>
        /// </summary>
        public override string Database => _connection.Database;

        /// <summary>
        /// Gets the data source file name without extension or path. 
        /// </summary>
        public override string DataSource => _connection.DataSource;

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
            return _connection.BeginTransaction(isolationLevel);
        }

        /// <summary>
        /// Creates a new <see cref="SQLiteTransaction"/> if one isn't already active on the connection.
        /// </summary>
        public DbTransaction BeginDbTransaction()
        {
            return _connection.BeginTransaction();
        }

        /// <summary>
        /// When the database connection is closed, all commands linked to this connection are automatically reset.
        /// <remarks>This method does nothing for an in-memory connection. See <see cref="CloseForce"/></remarks>
        /// </summary>
        public override void Close()
        {
            if (!IsInMemory)
            {
                _connection.Close();
            }
        }

        /// <summary>
        /// When the database connection is closed, all commands linked to this connection are automatically reset.
        /// </summary>
        public void CloseForce()
        {
            _connection.Close();
        }

        /// <summary>
        /// This method is not implemented; however, the <c>Changed</c> event will still be raised. 
        /// </summary>
        public override void ChangeDatabase(string databaseName)
        {
            _connection.ChangeDatabase(databaseName);
        }

        /// <summary>
        /// Create a new <see cref="SQLiteCommand"/> and associate it with this connection. 
        /// </summary>
        protected override DbCommand CreateDbCommand()
        {
            return _connection.CreateCommand();
        }

        /// <summary>
        /// Opens the connection using the parameters found in the <see cref="ConnectionString"/>. 
        /// </summary>
        public override void Open()
        {
            using (_locker.Lock(5.Seconds()))
            {
                if (ShouldRoll()) { Roll(); }
            }

            if (State == ConnectionState.Closed)
            {
                _connection.Open();
            }
        }

        /// <summary>
        /// Opens and returns the connection.
        /// </summary>
        public SqliteConnectionWrapper OpenAndReturn()
        {
            Open();
            return this;
        }

        /// <summary>
        /// Disposes and finalizes the connection, if applicable.
        /// </summary>
        public new void Dispose()
        {
            if (IsInMemory) { return; }
            if (_connection.State != ConnectionState.Closed) { _connection.Close(); }
            _connection.Dispose();
            _isDisposed = true;
        }

        private bool ShouldRoll()
        {
            if (IsInMemory) { return false; }
            return DateTime.Now - _connectionLastCreationTime >= RollEvery;
        }

        private void Roll()
        {
            if (State != ConnectionState.Open) { _connection.Open(); }
            var sqliteObjects = _connection.QueryAsync<SqliteObject>(SqliteSql.Master).Result;
            _connection.Close();

            var builder = new StringBuilder();
            foreach (var item in sqliteObjects)
            {
                builder.AppendLine(item.Sql + ';'.ToString());
            }

            var initializationQuery = builder.ToString();

            var connString = ConnectionString;
            var currentDbFile = SqliteHelper.GetDatabaseFile(connString);

            var now = DateTime.Now;
            var dbFileName = Path.GetFileNameWithoutExtension(currentDbFile.FullName);
            // ReSharper disable once UseFormatSpecifierInInterpolation
            var newName = $"{dbFileName}_[{(++RollCount).ToString()}][{now.ToString("yyyy-MM-dd-HH-mm-ss")}]{currentDbFile.Extension}";
            
            currentDbFile.Rename(newName);

            _connection = new SQLiteConnection(connString).OpenAndReturn();
            _connection.ExecuteAsync(initializationQuery).Wait();
            _connection.Close();
            _connectionLastCreationTime = _fromStartOfToday ? now.Date : now;
        }
    }
}