namespace Easy.Storage.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using Easy.Common;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.SqlServer.Models;

    /// <summary>
    /// Represents a <c>SQL Server</c> database.
    /// </summary>
    public sealed class SqlServerDatabase : Database
    {
        /// <summary>
        /// Creates an instance of the <see cref="SqlServerDatabase"/>.
        /// </summary>
        /// <param name="connectionString">A valid <c>SQL Server</c> connection-string.</param>
        public SqlServerDatabase(string connectionString) : this(new SqlConnection(connectionString)) {}

        /// <summary>
        /// Creates an instance of the <see cref="SqlServerDatabase"/>.
        /// </summary>
        /// <param name="connection">A <c>SQL Server</c> connection.</param>
        public SqlServerDatabase(SqlConnection connection)
        {
            Connection = Ensure.NotNull(connection, nameof(connection));
        }

        /// <summary>
        /// Gets the connection.
        /// </summary>
        public override IDbConnection Connection { get; }

        /// <summary>
        /// Returns the <c>SQL Server</c> objects in the database.
        /// </summary>
        public Task<IEnumerable<SqlServerObject>> GetDatabaseObjectsAsync()
        {
            return this.QueryAsync<SqlServerObject>(SqlServerSql.AllObjects);
        }

        /// <summary>
        /// Returns the information relating to the table represented by the <typeparamref name="T"/> in the <c>SQL Server</c> database.
        /// </summary>
        public Task<SqlServerTableInfo> GetTableInfoAsync<T>()
        {
            return GetTableInfoAsync(Table.Get<T>().Name);
        }

        /// <summary>
        /// Returns the information relating to the <paramref name="tableName"/>.
        /// </summary>
        public async Task<SqlServerTableInfo> GetTableInfoAsync(string tableName)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(tableName);

            var tableInfo = await this.QueryAsync<dynamic>(SqlServerSql.TableInfo, new { tableName })
                .ConfigureAwait(false);

            string database = null, schema = null;
            var counter = 0;

            var columnInfos = new List<SqlServerColumnInfo>();
            foreach (var i in tableInfo)
            {
                if (counter++ == 0)
                {
                    database = i.Database;
                    schema = i.Schema;
                }

                columnInfos.Add(new SqlServerColumnInfo
                {
                    Name = i.Name,
                    Type = Extensions.StringExtensions.ParseAsSqlServerDataType(i.TypeName.ToString()),
                    Precision = i.Precision,
                    Position = i.Position,
                    MaximumLength = i.MaximumLength,
                    Scale = i.Scale,
                    IsNullable = i.IsNullable == "YES",
                    Collation = i.Collation,
                    IsPrimaryKey = i.IsPrimaryKey == 1
                });
            }

            if (counter == 0)
            {
                throw new InvalidOperationException($"Table: {tableName} does not seem to exist.");
            }

            return new SqlServerTableInfo
            {
                Database = database,
                Schema = schema,
                Name = tableName,
                Columns = columnInfos.ToArray()
            };
        }

        /// <summary>
        /// Returns <c>True</c> if a table representing <typeparamref name="T"/> exists on the storage.
        /// </summary>
        public override async Task<bool> ExistsAsync<T>()
        {
            var tableName = Table.Get<T>().Name;
            return await this.ExecuteScalarAsync<uint>(SqlServerSql.TableExists, new { tableName })
                .ConfigureAwait(false) != 0;
        }

        /// <summary>
        /// Gets an instance of the <see cref="Repository{T}"/> for the given <typeparamref name="T"/>.
        /// </summary>
        public override IRepository<T> GetRepository<T>()
        {
            return new Repository<T>(Connection, Dialect.SqlServer);
        }

        /// <summary>
        /// Begins a database transaction.
        /// </summary>
        public override IDbTransaction BeginTransaction()
        {
            if (Connection.State != ConnectionState.Open) { Connection.Open(); }

            return Connection.BeginTransaction();
        }

        /// <summary>
        /// Begins a database transaction with the specified IsolationLevel value.
        /// </summary>
        public override IDbTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            if (Connection.State != ConnectionState.Open) { Connection.Open(); }

            return Connection.BeginTransaction(isolationLevel);
        }

        /// <summary>
        /// Releases the resources used by this instance.
        /// </summary>
        public override void Dispose()
        {
            Connection.Dispose();
        }
    }
}
