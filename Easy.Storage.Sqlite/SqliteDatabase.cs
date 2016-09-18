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
        /// <param name="rollBy">The <see cref="TimeSpan"/> by which the connection database file should be rolled.</param>
        public SqliteDatabase(string connectionString, TimeSpan? rollBy = null)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(connectionString);
            _connection = new SqliteConnectionWrapper(new SQLiteConnection(connectionString), rollBy);
        }

        /// <summary>
        /// Gets the connection.
        /// </summary>
        public override IDbConnection Connection => _connection.OpenAndReturn();

        /// <summary>
        /// Gets an instance of the <see cref="Repository{T}"/> for the given <typeparamref name="T"/>.
        /// </summary>
        /// <param name="singleWriter">
        /// The flag indicating whether a single writer/updater/deleter <see cref="Repository{T}"/> should be returned.
        /// </param>
        public override IRepository<T> GetRepository<T>(bool singleWriter = false)
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
            return Connection.QueryAsync<SqliteObject>(SqliteSql.Master);
        }

        /// <summary>
        /// Returns <c>True</c> if a table representing <typeparamref name="T"/> exists on the storage.
        /// </summary>
        public override async Task<bool> ExistsAsync<T>()
        {
            var sqliteTableName = Table.Get<T>().Name.Trim('[', ']');
            return await Connection.ExecuteScalarAsync<uint>(SqliteSql.TableExists, new { tableName = sqliteTableName })
                .ConfigureAwait(false) != 0;
        }

        /// <summary>
        /// Returns the information relating to the table represented by the <typeparamref name="T"/> in the <c>SQLite</c> database.
        /// </summary>
        public async Task<SqliteTableInfo> GetTableInfoAsync<T>()
        {
            var sqliteTableName = Table.Get<T>().Name.Trim('[', ']');

            IEnumerable<dynamic> tableInfo;
            try
            {
                tableInfo = await Connection.QueryAsync<dynamic>($"PRAGMA table_info({sqliteTableName})").ConfigureAwait(false);
            }
            catch (InvalidOperationException e)
            {
                throw new InvalidOperationException($"Table: {sqliteTableName} does not seem to exist.", e);
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
                    throw new ArgumentOutOfRangeException(i.type, "Invalid column type of: " + typeStr);
                }

                return new SqliteColumnInfo
                {
                    TableName = sqliteTableName,
                    ColumnId = i.cid,
                    ColumnName = i.name,
                    ColumnTpe = columnType,
                    NotNull = i.notnull == 1,
                    DefaultValue = i.dflt_value,
                    IsPrimaryKey = i.pk == 1
                };

            }).ToArray();

            var databaseObjects = await GetDatabaseObjectsAsync();

            return new SqliteTableInfo
            {
                TableName = sqliteTableName,
                Sql = databaseObjects.Single(x => x.Type == SqliteObjectType.Table && x.Name == sqliteTableName).Sql,
                Columns = columnsInfo
            };
        }

        /// <summary>
        /// Returns records matching the given <paramref name="term"/>.
        /// </summary>
        public Task<IEnumerable<T>> SearchAsync<T>(ITerm<T> term)
        {
            var query = Table.Get<T>().Select.Replace($"{Formatter.Spacer}1 = 1;", $"rowId IN {Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{term}{Formatter.NewLine});");
            return Connection.QueryAsync<T>(query);
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
