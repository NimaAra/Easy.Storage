namespace Easy.Storage.Common
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Dapper;
    using Easy.Common;
    using Easy.Storage.Common.Extensions;

    /// <summary>
    /// Represents a repository for retrieving records of the given <typeparamref name="T"/> type.
    /// </summary>
    public class Repository<T> : IRepository<T>
    {
        private readonly Table _table;

        /// <summary>
        /// The <see cref="IDbConnection"/> used by the <see cref="Repository{T}"/>.
        /// </summary>
        protected readonly IDbConnection Connection;

        /// <summary>
        /// Creates an instance of the <see cref="Repository{T}"/>.
        /// </summary>
        internal Repository(IDbConnection dbConnection, Dialect dialect = Dialect.Generic)
        {
            Connection = Ensure.NotNull(dbConnection, nameof(dbConnection));
            _table = Table.Get<T>(dialect);
        }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        public Task<IEnumerable<T>> GetAsync(IDbTransaction transaction = null)
        {
            return DbConnectionExtensions.QueryAsync<T>(Connection, _table.Select, transaction: transaction);
        }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        public Task<IEnumerable<T>> GetLazyAsync(IDbTransaction transaction = null)
        {
            var tcs = new TaskCompletionSource<IEnumerable<T>>();
            var result = Connection.Query<T>(_table.Select, buffered: false, transaction: transaction);
            tcs.SetResult(result);
            return tcs.Task;
        }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="value">The value associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        public Task<IEnumerable<T>> GetAsync<TProperty>(Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var query = _table.GetSqlWithClause(selector, _table.Select, true);
            return DbConnectionExtensions.QueryAsync<T>(Connection, query, new { Value = value }, transaction: transaction);
        }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <param name="values">The values associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        public Task<IEnumerable<T>> GetAsync<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values)
        {
            Ensure.NotNull(selector, nameof(selector));
            Ensure.NotNull(values, nameof(values));

            var query = _table.GetSqlWithClause(selector, _table.Select, false);
            return DbConnectionExtensions.QueryAsync<T>(Connection, query, new { Values = values }, transaction: transaction);
        }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> based on the given <paramref name="queryFilter"/>.
        /// </summary>
        /// <param name="transaction">The transaction</param>
        /// <param name="queryFilter">The filter to limit the records returned</param>
        public Task<IEnumerable<T>> GetAsync(QueryFilter<T> queryFilter, IDbTransaction transaction = null)
        {
            Ensure.NotNull(queryFilter, nameof(queryFilter));
            return DbConnectionExtensions.QueryAsync<T>(Connection, queryFilter.Query, queryFilter.Parameters, transaction: transaction);
        }

        /// <summary>
        /// Inserts the given <paramref name="item"/> to the storage.
        /// </summary>
        /// <returns>The inserted id of the <paramref name="item"/>.</returns>
        public virtual async Task<long> InsertAsync(T item, IDbTransaction transaction = null)
        {
            return (await DbConnectionExtensions.QueryAsync<long>(Connection, _table.Insert, item, transaction: transaction)
                .ConfigureAwait(false)).First();
        }

        /// <summary>
        /// Inserts the given <paramref name="items"/> to the storage.
        /// </summary>
        /// <returns>The number of inserted records.</returns>
        public virtual Task<int> InsertAsync(IEnumerable<T> items, IDbTransaction transaction = null)
        {
            Ensure.NotNull(items, nameof(items));
            return DbConnectionExtensions.ExecuteAsync(Connection, _table.Insert, items, transaction: transaction);
        }

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the value of the id in the storage.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> UpdateAsync(T item, IDbTransaction transaction = null)
        {
            return DbConnectionExtensions.ExecuteAsync(Connection, _table.UpdateDefault, item, transaction: transaction);
        }

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the value of the <paramref name="selector"/>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> UpdateAsync<TProperty>(T item, Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null)
        {
            var parameters = new DynamicParameters(item);
            parameters.Add("Value", value);
            var query = _table.GetSqlWithClause(selector, _table.UpdateCustom, true);
            
            return DbConnectionExtensions.ExecuteAsync(Connection, query, parameters, transaction: transaction);
        }

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the values of the <paramref name="selector"/>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> UpdateAsync<TProperty>(T item, Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values)
        {
            var parameters = new DynamicParameters(item);
            parameters.Add("Values", values);
            var query = _table.GetSqlWithClause(selector, _table.UpdateCustom, false);

            return DbConnectionExtensions.ExecuteAsync(Connection, query, parameters, transaction: transaction);
        }

        /// <summary>
        /// Updates the given <paramref name="items"/> based on the value of their ids in the storage.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> UpdateAsync(IEnumerable<T> items, IDbTransaction transaction = null)
        {
            return DbConnectionExtensions.ExecuteAsync(Connection, _table.UpdateDefault, items, transaction: transaction);
        }

        /// <summary>
        /// Deletes a record based on the value of the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="value">The value associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> DeleteAsync<TProperty>(Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var query = _table.GetSqlWithClause(selector, _table.Delete, true);
            return DbConnectionExtensions.ExecuteAsync(Connection, query, new { Value = value }, transaction: transaction);
        }

        /// <summary>
        /// Deletes a record based on the value of the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <param name="values">The values associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> DeleteAsync<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values)
        {
            Ensure.NotNull(selector, nameof(selector));
            Ensure.NotNull(values, nameof(values));

            var query = _table.GetSqlWithClause(selector, _table.Delete, false);
            return DbConnectionExtensions.ExecuteAsync(Connection, query, new { Values = values }, transaction: transaction);
        }

        /// <summary>
        /// Deletes all the records.
        /// </summary>
        public virtual Task<int> DeleteAllAsync(IDbTransaction transaction = null)
        {
            return DbConnectionExtensions.ExecuteAsync(Connection, _table.Delete, transaction: transaction);
        }

        /// <summary>
        /// Returns the count of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>COUNT</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        public Task<ulong> CountAsync<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = _table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT COUNT ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {_table.Name}";
            return DbConnectionExtensions.ExecuteScalarAsync<ulong>(Connection, query, transaction: transaction);
        }

        /// <summary>
        /// Returns the sum of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>SUM</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        public Task<long> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = _table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT SUM ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {_table.Name}";
            return DbConnectionExtensions.ExecuteScalarAsync<long>(Connection, query, transaction: transaction);
        }

        /// <summary>
        /// Returns the average of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>AVG</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        public Task<decimal> AvgAsync<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = _table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT AVG ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {_table.Name}";
            return DbConnectionExtensions.ExecuteScalarAsync<decimal>(Connection, query, transaction: transaction);
        }

        /// <summary>
        /// Returns the minimum value defined by the given <paramref name="selector"/>.
        /// </summary>
        public Task<TProperty> MinAsync<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = _table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT MIN ({column}) FROM {_table.Name}";
            return DbConnectionExtensions.ExecuteScalarAsync<TProperty>(Connection, query, transaction: transaction);
        }

        /// <summary>
        /// Returns the maximum value defined by the given <paramref name="selector"/>.
        /// </summary>
        public Task<TProperty> MaxAsync<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = _table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT MAX ({column}) FROM {_table.Name}";
            return DbConnectionExtensions.ExecuteScalarAsync<TProperty>(Connection, query, transaction: transaction);
        }
    }
}