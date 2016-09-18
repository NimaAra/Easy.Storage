namespace Easy.Storage.Common
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Easy.Common;
    using Easy.Storage.Common.Extensions;

    /// <summary>
    /// Represents a repository for retrieving records of the given <typeparamref name="T"/> type.
    /// </summary>
    public class Repository<T> : IRepository<T>
    {
        private readonly IDbConnection _db;
        private readonly Table _table;

        /// <summary>
        /// Creates an instance of the <see cref="Repository{T}"/>.
        /// </summary>
        internal Repository(IDbConnection dbConnection)
        {
            _db = Ensure.NotNull(dbConnection, nameof(dbConnection));
            _table = Table.Get<T>();
        }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        public Task<IEnumerable<T>> GetAsync(IDbTransaction transaction = null)
        {
            return _db.QueryAsync<T>(_table.Select, transaction: transaction);
        }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        public Task<IEnumerable<T>> GetLazyAsync(IDbTransaction transaction = null)
        {
            var tcs = new TaskCompletionSource<IEnumerable<T>>();
            var result = Dapper.SqlMapper.Query<T>(_db, _table.Select, buffered: false, transaction: transaction);
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
            return _db.QueryAsync<T>(query, new { Value = value }, transaction);
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
            return _db.QueryAsync<T>(query, new { Values = values }, transaction);
        }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> based on the given <paramref name="queryFilter"/>.
        /// </summary>
        /// <param name="transaction">The transaction</param>
        /// <param name="queryFilter">The filter to limit the records returned</param>
        public Task<IEnumerable<T>> GetAsync(QueryFilter<T> queryFilter, IDbTransaction transaction = null)
        {
            Ensure.NotNull(queryFilter, nameof(queryFilter));
            return _db.QueryAsync<T>(queryFilter.Query, queryFilter.Parameters, transaction);
        }

        /// <summary>
        /// Inserts the given <paramref name="item"/> to the storage.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> InsertAsync(T item, IDbTransaction transaction = null)
        {
            return _db.ExecuteAsync(_table.Insert, item, transaction);
        }

        /// <summary>
        /// Inserts the given <paramref name="items"/> to the storage.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> InsertAsync(IEnumerable<T> items, IDbTransaction transaction = null)
        {
            Ensure.NotNull(items, nameof(items));
            return _db.ExecuteAsync(_table.Insert, items, transaction);
        }

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the value of the id in the storage.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> UpdateAsync(T item, IDbTransaction transaction = null)
        {
            return _db.ExecuteAsync(_table.UpdateDefault, item, transaction);
        }

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the value of the <paramref name="selector"/>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> UpdateAsync<TProperty>(T item, Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null)
        {
            var parameters = new Dapper.DynamicParameters(item);
            parameters.Add("Value", value);
            var query = _table.GetSqlWithClause(selector, _table.UpdateCustom, true);
            
            return _db.ExecuteAsync(query, parameters, transaction);
        }

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the values of the <paramref name="selector"/>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> UpdateAsync<TProperty>(T item, Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values)
        {
            var parameters = new Dapper.DynamicParameters(item);
            parameters.Add("Values", values);
            var query = _table.GetSqlWithClause(selector, _table.UpdateCustom, false);

            return _db.ExecuteAsync(query, parameters, transaction);
        }

        /// <summary>
        /// Updates the given <paramref name="items"/> based on the value of their ids in the storage.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public virtual Task<int> UpdateAsync(IEnumerable<T> items, IDbTransaction transaction = null)
        {
            return _db.ExecuteAsync(_table.UpdateDefault, items, transaction);
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
            return _db.ExecuteAsync(query, new { Value = value }, transaction);
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
            return _db.ExecuteAsync(query, new { Values = values }, transaction);
        }

        /// <summary>
        /// Deletes all the records.
        /// </summary>
        public virtual Task<int> DeleteAllAsync(IDbTransaction transaction = null)
        {
            return _db.ExecuteAsync(_table.Delete, transaction);
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

            var column = _table.GetColumnName(selector.GetPropertyName());
            var query = $"SELECT COUNT ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {_table.Name}";
            return _db.ExecuteScalarAsync<ulong>(query, transaction);
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

            var column = _table.GetColumnName(selector.GetPropertyName());
            var query = $"SELECT SUM ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {_table.Name}";
            return _db.ExecuteScalarAsync<long>(query, transaction);
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

            var column = _table.GetColumnName(selector.GetPropertyName());
            var query = $"SELECT AVG ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {_table.Name}";
            return _db.ExecuteScalarAsync<decimal>(query, transaction);
        }

        /// <summary>
        /// Returns the minimum value defined by the given <paramref name="selector"/>.
        /// </summary>
        public Task<TProperty> MinAsync<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = _table.GetColumnName(selector.GetPropertyName());
            var query = $"SELECT MIN ({column}) FROM {_table.Name}";
            return _db.ExecuteScalarAsync<TProperty>(query, transaction);
        }

        /// <summary>
        /// Returns the maximum value defined by the given <paramref name="selector"/>.
        /// </summary>
        public Task<TProperty> MaxAsync<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = _table.GetColumnName(selector.GetPropertyName());
            var query = $"SELECT MAX ({column}) FROM {_table.Name}";
            return _db.ExecuteScalarAsync<TProperty>(query, transaction);
        }

        /// <summary>
        /// Releases the resources used by this instance.
        /// </summary>
        public virtual void Dispose()
        {
            // does nothing
        }
    }
}