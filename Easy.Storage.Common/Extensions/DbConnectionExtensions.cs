namespace Easy.Storage.Common.Extensions
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Dapper;
    using Easy.Storage.Common.TypeHandlers;

    /// <summary>
    /// Provides a set of helper methods for when working with <see cref="IDbConnection"/>.
    /// </summary>
    public static class DbConnectionExtensions
    {
        static DbConnectionExtensions()
        {
            SqlMapper.AddTypeHandler(new GuidHandler());
            SqlMapper.AddTypeHandler(new DateTimeOffsetHandler());
            SqlMapper.AddTypeMap(typeof(DateTime), DbType.DateTime2);
            SqlMapper.AddTypeMap(typeof(DateTime?), DbType.DateTime2);
        }

        /// <summary>
        /// Gets an instance of the <see cref="StorageContext{T}"/> for the given <typeparamref name="T"/>.
        /// <param name="connection">The database connection.</param>
        /// <param name="dialect">The dialect to use for generating <c>SQL</c> DDL and DML queries.</param>
        /// </summary>
        public static IStorageContext<T> GetStorageContext<T>(this IDbConnection connection, Dialect dialect)
        {
            return new StorageContext<T>(connection, dialect);
        }

        /// <summary>
        /// Executes the given <paramref name="sql"/>.
        /// </summary>
        /// <returns>The number of rows affected</returns>
        public static Task<int> ExecuteAsync(
            this IDbConnection connection,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null,
            CancellationToken cToken = default(CancellationToken))
        {
            return connection.ExecuteAsync(new CommandDefinition(sql, param, transaction, commandTimeout, commandType, CommandFlags.None, cToken));
        }

        /// <summary>
        /// Executes the given <paramref name="sql"/> and returns the result of the query.
        /// </summary>
        public static Task<IEnumerable<TReturn>> QueryAsync<TReturn>(
            this IDbConnection connection, 
            string sql, 
            object param = null, 
            IDbTransaction transaction = null, 
            int? commandTimeout = null, 
            CommandType? commandType = null, 
            bool buffered = true, 
            CancellationToken cToken = default(CancellationToken))
        {
            return connection.QueryAsync<TReturn>(new CommandDefinition(sql, param, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None, cToken));
        }

        /// <summary>
        /// Executes the given <paramref name="sql"/> that returns a single value.
        /// </summary>
        public static Task<TReturn> ExecuteScalarAsync<TReturn>(
            this IDbConnection connection,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null,
            CancellationToken cToken = default(CancellationToken))
        {
            return connection.ExecuteScalarAsync<TReturn>(new CommandDefinition(sql, param, transaction, commandTimeout, commandType, CommandFlags.None, cToken));
        }

        /// <summary>
        /// Execute a command that returns multiple result sets, and access each in turn.
        /// <remarks>
        /// This method is not supported by <c>Oracle</c> as per: <see href="http://stackoverflow.com/a/6338193"/>.
        /// </remarks>
        /// </summary>
        public static async Task<Reader> QueryMultipleAsync(
            this IDbConnection connection,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null,
            bool buffered = true,
            CancellationToken cToken = default(CancellationToken))
        {
            return new Reader(await connection.QueryMultipleAsync(new CommandDefinition(sql, param, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None, cToken)));
        }
    }
}