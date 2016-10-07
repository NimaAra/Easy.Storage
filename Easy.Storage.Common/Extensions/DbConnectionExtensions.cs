namespace Easy.Storage.Common.Extensions
{
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Dapper;

    /// <summary>
    /// Provides a set of helper methods for when working with <see cref="IDbConnection"/>.
    /// </summary>
    public static class DbConnectionExtensions
    {
        /// <summary>
        /// Executes the given <paramref name="sql"/>.
        /// </summary>
        /// <returns>The number of rows affected</returns>
        internal static Task<int> ExecuteAsync(
            this IDbConnection conn,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null,
            CancellationToken cToken = default(CancellationToken))
        {
            return conn.ExecuteAsync(new CommandDefinition(sql, param, transaction, commandTimeout, commandType, CommandFlags.None, cToken));
        }

        /// <summary>
        /// Executes the given <paramref name="sql"/> and returns the result of the query.
        /// </summary>
        internal static Task<IEnumerable<TReturn>> QueryAsync<TReturn>(
            this IDbConnection conn, 
            string sql, 
            object param = null, 
            IDbTransaction transaction = null, 
            int? commandTimeout = null, 
            CommandType? commandType = null, 
            bool buffered = true, 
            CancellationToken cToken = default(CancellationToken))
        {
            return conn.QueryAsync<TReturn>(new CommandDefinition(sql, param, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None, cToken));
        }

        /// <summary>
        /// Executes the given <paramref name="sql"/> that returns a single value.
        /// </summary>
        /// <returns>The first cell selected</returns>
        internal static Task<TReturn> ExecuteScalarAsync<TReturn>(
            this IDbConnection conn,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null,
            CancellationToken cToken = default(CancellationToken))
        {
            return conn.ExecuteScalarAsync<TReturn>(new CommandDefinition(sql, param, transaction, commandTimeout, commandType, CommandFlags.None, cToken));
        }
    }
}