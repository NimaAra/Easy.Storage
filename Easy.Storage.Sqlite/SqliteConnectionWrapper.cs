namespace Easy.Storage.Sqlite
{
    using Easy.Common.Extensions;
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Data.SQLite;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;
    using Easy.Common;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.Sqlite.Models;

    /// <summary>
    /// A wrapper around the <see cref="SQLiteConnection"/> to allow reuse of in-memory connections.
    /// </summary>
    public sealed class SqliteConnectionWrapper : DbConnection
    {
        private SQLiteConnection _connection;
        private volatile bool _isDisposed;
        private DateTime _connectionLastCreationTime;
        private readonly bool _fromStartOfToday;
        private readonly FileInfo _originalDbFile;
        private readonly string _originalConnString;
        private readonly bool _isRollable = true;

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
            IsInMemory = SqliteHelper.IsInMemoryConnection(_connection.ConnectionString);
            RollEvery = rollEvery?? TimeSpan.MaxValue;

            if (IsInMemory || RollEvery == TimeSpan.MaxValue) { _isRollable = false; }

            Ensure.That<ArgumentOutOfRangeException>(RollEvery >= 1.Seconds(), $"{nameof(rollEvery)} cannot be less than 1 second.");

            _originalConnString = _connection.ConnectionString;
            _originalDbFile = SqliteHelper.GetDatabaseFile(_originalConnString);

            var now = DateTime.Now;
            _connectionLastCreationTime = _fromStartOfToday ? now.Date : now;

            if (!_isRollable) { return; }

            var rolledDbFile = GetRollingDataBaseFile(_connectionLastCreationTime, ++RollCount, _originalDbFile);
            var newConnString = _originalConnString.Replace(_originalDbFile.FullName, rolledDbFile.FullName);
            _connection = new SQLiteConnection(newConnString);
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
            if (State == ConnectionState.Closed) { _connection.Open(); }
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

        internal bool ShouldRoll()
        {
            if (!_isRollable) { return false; }
            return DateTime.Now - _connectionLastCreationTime >= RollEvery;
        }

        internal async void Roll()
        {
            var now = DateTime.Now;

            var initializationQuery = await GetInitializationQuery(_connection);

            var rolledDbFile = GetRollingDataBaseFile(now, ++RollCount, _originalDbFile);
            var newConnString = _originalConnString.Replace(_originalDbFile.FullName, rolledDbFile.FullName);

            _connection = new SQLiteConnection(newConnString).OpenAndReturn();
            await _connection.ExecuteAsync(initializationQuery).ConfigureAwait(false);
            _connection.Close();
            _connectionLastCreationTime = _fromStartOfToday ? now.Date : now;
        }

        private static FileInfo GetRollingDataBaseFile(DateTime dateTime, uint rollCount, FileInfo originalDbFile)
        {
            var dbFileName = Path.GetFileNameWithoutExtension(originalDbFile.FullName);
            var dbDirectoryName = originalDbFile.DirectoryName;

            // ReSharper disable once UseFormatSpecifierInInterpolation
            // ReSharper disable once AssignNullToNotNullAttribute
            var newDbFilePath = Path.Combine(dbDirectoryName, $"{dbFileName}_[{rollCount.ToString()}][{dateTime.ToString("yyyy-MM-dd-HH-mm-ss")}]{originalDbFile.Extension}");
            return new FileInfo(newDbFilePath);
        }

        private async Task<string> GetInitializationQuery(IDbConnection connection)
        {
            if (State != ConnectionState.Open) { connection.Open(); }
            var sqliteObjects = await connection.QueryAsync<SqliteObject>(SqliteSql.Master).ConfigureAwait(false);
            connection.Close();

            var builder = new StringBuilder();
            foreach (var item in sqliteObjects)
            {
                builder.AppendLine(item.Sql + ";");
            }

            return builder.ToString();
        }
    }
}