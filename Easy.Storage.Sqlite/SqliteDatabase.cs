namespace Easy.Storage.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SQLite;
    using System.Linq;
    using System.Threading.Tasks;
    using Easy.Common;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.Sqlite.FTS;
    using Easy.Storage.Sqlite.Models;

    /// <summary>
    /// Represents a <c>SQLite</c> database.
    /// </summary>
    public sealed class SqliteDatabase : Database
    {
        private readonly SqliteConnectionWrapper _connection;

        /// <summary>
        /// Creates an instance of the <see cref="SqliteDatabase"/>.
        /// </summary>
        /// <param name="connectionString">A valid <c>SQLite</c> connection-string</param>
        public SqliteDatabase(string connectionString) 
            : this(new SqliteConnectionWrapper(new SQLiteConnection(connectionString))) {}

        /// <summary>
        /// Creates an instance of the <see cref="SqliteDatabase"/>.
        /// </summary>
        /// <param name="connection">A valid <c>SQLite</c> connection wrapped inside <see cref="SqliteConnectionWrapper"/>.</param>
        public SqliteDatabase(SqliteConnectionWrapper connection)
        {
            _connection = Ensure.NotNull(connection, nameof(connection));
        }

        /// <summary>
        /// Gets the connection.
        /// </summary>
        public override IDbConnection Connection => _connection.OpenAndReturn();

        /// <summary>
        /// Gets an instance of the <see cref="Repository{T}"/> for the given <typeparamref name="T"/>.
        /// </summary>
        public override IRepository<T> GetRepository<T>()
        {
            return new SingleWriterRepository<T>(Connection);
        }

        /// <summary>
        /// Gets a new <see cref="IDbTransaction"/> if one isn't already active on the connection.
        /// </summary>
        public override IDbTransaction BeginTransaction()
        {
            return Connection.BeginTransaction();
        }

        /// <summary>
        /// Gets a new <see cref="IDbTransaction"/> if one isn't already active on the connection.
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
        public override IDbTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            return Connection.BeginTransaction(isolationLevel);
        }

        /// <summary>
        /// Returns the <c>SQLite</c> objects in the database.
        /// </summary>
        public Task<IEnumerable<SqliteObject>> GetDatabaseObjectsAsync()
        {
            return this.QueryAsync<SqliteObject>(SqliteSql.Master);
        }

        /// <summary>
        /// Returns <c>True</c> if a table representing <typeparamref name="T"/> exists on the storage.
        /// </summary>
        public override async Task<bool> ExistsAsync<T>()
        {
            var tableName = Table.Get<T>().Name;
            return await this.ExecuteScalarAsync<uint>(SqliteSql.TableExists, new {tableName }).ConfigureAwait(false) != 0;
        }

        /// <summary>
        /// Returns the information relating to the table represented by the <typeparamref name="T"/> in the <c>SQLite</c> database.
        /// </summary>
        public Task<SqliteTableInfo> GetTableInfoAsync<T>()
        {
            return GetTableInfoAsync(Table.Get<T>().Name);
        }

        /// <summary>
        /// Returns the information relating to the <paramref name="tableName"/>.
        /// </summary>
        public async Task<SqliteTableInfo> GetTableInfoAsync(string tableName)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(tableName);

            IEnumerable<dynamic> tableInfo;
            try
            {
                tableInfo = await this.QueryAsync<dynamic>($"PRAGMA table_info({tableName})").ConfigureAwait(false);
            }
            catch (InvalidOperationException e)
            {
                throw new InvalidOperationException($"Table: {tableName} does not seem to exist.", e);
            }

            var columnsInfo = tableInfo.Select(i =>
            {
                SqliteDataType columnType;
                string typeStr = i.type.ToString();
                if (typeStr.Equals("INTEGER", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SqliteDataType.INTEGER;
                }
                else if (typeStr.Equals("REAL", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SqliteDataType.REAL;
                }
                else if (typeStr.Equals("TEXT", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SqliteDataType.TEXT;
                }
                else if (typeStr.Equals("BLOB", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SqliteDataType.BLOB;
                }
                else if (typeStr.Equals("NULL", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SqliteDataType.NULL;
                }
                else
                {
                    throw new ArgumentOutOfRangeException(nameof(i.type), "Invalid column type of: " + typeStr);
                }

                return new SqliteColumnInfo
                {
                    TableName = tableName,
                    Id = i.cid,
                    Name = i.name,
                    Type = columnType,
                    NotNull = i.notnull == 1,
                    DefaultValue = i.dflt_value,
                    IsPrimaryKey = i.pk == 1
                };

            }).ToArray();

            var databaseObjects = await GetDatabaseObjectsAsync();

            return new SqliteTableInfo
            {
                TableName = tableName,
                Sql = databaseObjects.Single(x => x.Type == SqliteObjectType.Table && x.Name == tableName).Sql,
                Columns = columnsInfo
            };
        }

        /// <summary>
        /// Returns records matching the given <paramref name="term"/>.
        /// </summary>
        public Task<IEnumerable<T>> SearchAsync<T>(ITerm<T> term)
        {
            var query = Table.Get<T>().Select.Replace($"{Formatter.Spacer}1 = 1;", $"rowId IN {Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{term}{Formatter.NewLine});");
            return this.QueryAsync<T>(query);
        }

        /// <summary>
        /// Releases the resources used by this instance.
        /// </summary>
        public override void Dispose()
        {
            ((SqliteConnectionWrapper)Connection).Dispose();
        }
    }
}
