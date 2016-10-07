namespace Easy.Storage.Common.Extensions
{
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides a set of helper methods for when working with <see cref="IDatabase"/>.
    /// </summary>
    public static class DatabaseExtensions
    {
        /// <summary>
        /// Executes the given <paramref name="sql"/>.
        /// </summary>
        /// <returns>The number of rows affected</returns>
        public static Task<int> ExecuteAsync(
            this IDatabase db,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null,
            CancellationToken cToken = default(CancellationToken))
        {
            return db.Connection.ExecuteAsync(sql, param, transaction, commandTimeout, commandType, cToken);
        }

        /// <summary>
        /// Executes the given <paramref name="sql"/> and returns the result of the query.
        /// </summary>
        public static Task<IEnumerable<TReturn>> QueryAsync<TReturn>(
            this IDatabase db, 
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null,
            bool buffered = true,
            CancellationToken cToken = default(CancellationToken))
        {   
            return db.Connection.QueryAsync<TReturn>(sql, param, transaction, commandTimeout, commandType, buffered, cToken);
        }
        
        /// <summary>
        /// Executes the given <paramref name="sql"/> that returns a single value.
        /// </summary>
        /// <returns>The first cell selected</returns>
        public static Task<TReturn> ExecuteScalarAsync<TReturn>(
            this IDatabase db,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null,
            CancellationToken cToken = default(CancellationToken))
        {
            return db.Connection.ExecuteScalarAsync<TReturn>(sql, param, transaction, commandTimeout, commandType, cToken);
        }
    }
}