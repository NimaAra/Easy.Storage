namespace Easy.Storage.Common.Extensions
{
    using System.Collections.Generic;
    using System.Data;
    using System.Threading.Tasks;
    using Dapper;

    /// <summary>
    /// Provides a set of helper methods for when working with <see cref="IDbConnection"/>.
    /// </summary>
    public static class DbExtensions
    {
        /// <summary>
        /// Executes the given <paramref name="sql"/>.
        /// </summary>
        /// <returns>The number of rows affected</returns>
        public static Task<int> ExecuteAsync(this IDbConnection db, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return SqlMapper.ExecuteAsync(db, sql, param, transaction, commandTimeout, commandType);
        }

        /// <summary>
        /// Executes the given <paramref name="sql"/> and returns the result of the query.
        /// </summary>
        public static Task<IEnumerable<TReturn>> QueryAsync<TReturn>(this IDbConnection db, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return SqlMapper.QueryAsync<TReturn>(db, sql, param, transaction, commandTimeout, commandType);
        }
        
        /// <summary>
        /// Executes the given <paramref name="sql"/> that returns a single value.
        /// </summary>
        /// <returns>The first cell selected</returns>
        public static Task<TReturn> ExecuteScalarAsync<TReturn>(this IDbConnection db, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return SqlMapper.ExecuteScalarAsync<TReturn>(db, sql, param, transaction, commandTimeout, commandType);
        }
    }
}