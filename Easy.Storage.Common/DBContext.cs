// ReSharper disable RedundantArgumentDefaultValue
namespace Easy.Storage.Common
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Easy.Common;
    using Easy.Common.Extensions;
    using Easy.Storage.Common.Attributes;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.Common.Filter;

    /// <summary>
    /// Represents the context for retrieving records of the given <typeparamref name="T"/> 
    /// type from the storage.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public sealed class DBContext<T> : IDBContext<T>
    {
        private readonly Task<int> _cachedZeroTask = Task.FromResult(0);

        /// <summary>
        /// Creates an instance of the <see cref="DBContext{T}"/>.
        /// </summary>
        internal DBContext(IDbConnection dbConnection, Dialect dialect, string tableName = "")
        {
            Connection = Ensure.NotNull(dbConnection, nameof(dbConnection));
            Table = Table.MakeOrGet<T>(dialect, tableName);
        }

        /// <summary>
        /// Gets the underlying connection.
        /// </summary>
        public IDbConnection Connection { get; }

        /// <summary>
        /// Gets the abstraction used for working with the model.
        /// </summary>
        public Table Table { get; }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// <remarks>This method returns a buffered result.</remarks>
        /// </summary>
        public Task<IEnumerable<T>> Get(IDbTransaction transaction = null)
            => Connection.QueryAsync<T>(Table.Select, transaction: transaction, buffered: true);

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// <remarks>This method returns a non-buffered result.</remarks>
        /// </summary>
        public Task<IEnumerable<T>> GetLazy(IDbTransaction transaction = null)
            => Connection.QueryAsync<T>(Table.Select, transaction: transaction, buffered: false);

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// <remarks>This method returns a buffered result.</remarks>
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="value">The value associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        public Task<IEnumerable<T>> GetWhere<TProperty>(Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var filter = Query<T>.Filter.And(selector, Operator.Equal, value);
            var sql = filter.GetSQL();
            var parameters = filter.Parameters.ToDynamicParameters();
            return Connection.QueryAsync<T>(sql, parameters, transaction: transaction, buffered: true);
        }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// <remarks>This method returns a buffered result.</remarks>
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <param name="values">The values associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        public Task<IEnumerable<T>> GetWhere<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values)
        {
            Ensure.NotNull(selector, nameof(selector));
            Ensure.NotNull(values, nameof(values));

            var filter = Query<T>.Filter.AndIn(selector, values);
            var sql = filter.GetSQL();
            var parameters = filter.Parameters.ToDynamicParameters();
            return Connection.QueryAsync<T>(sql, parameters, transaction: transaction, buffered: true);
        }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> based on the given <paramref name="filter"/>.
        /// <remarks>This method returns a buffered result.</remarks>
        /// </summary>
        /// <param name="transaction">The transaction</param>
        /// <param name="filter">The filter to limit the records returned</param>
        public Task<IEnumerable<T>> GetWhere(Filter<T> filter, IDbTransaction transaction = null)
        {
            Ensure.NotNull(filter, nameof(filter));

            var sql = filter.GetSQL();
            var parameters = filter.Parameters.ToDynamicParameters();
            return Connection.QueryAsync<T>(sql, parameters, transaction: transaction, buffered: true);
        }

        /// <summary>
        /// Inserts the given <paramref name="item"/> to the storage.
        /// </summary>
        /// <param name="item">The item to be inserted.</param>
        /// <param name="modelHasIdentityColumn">
        /// The flag indicating whether the model has an identity column specified. If <c>True</c>,
        /// The property of the model named <c>ID</c> or marked with the <see cref="IdentityAttribute"/>
        /// will not be inserted as it's value will be generated by the backing database. If this 
        /// flag is set to <c>False</c>, then every property value not marked with the <see cref="IgnoreAttribute"/>
        /// will be stored into the database.
        /// </param>
        /// <param name="transaction">The transaction</param>
        /// <returns>The inserted id of the <paramref name="item"/>.</returns>
        public async Task<object> Insert(T item, bool modelHasIdentityColumn = true, IDbTransaction transaction = null)
        {
            var insertSql = modelHasIdentityColumn ? Table.InsertIdentity : Table.InsertAll;
            return (await Connection.QueryAsync<dynamic>(insertSql, item, transaction: transaction, buffered: true)
                .ConfigureAwait(false)).First().Id;
        }

        /// <summary>
        /// Inserts the given <paramref name="items"/> to the storage.
        /// </summary>
        /// <param name="items">The items to be inserted.</param>
        /// <param name="modelHasIdentityColumn">
        /// The flag indicating whether the model has an identity column specified. If <c>True</c>,
        /// The property of the model named <c>ID</c> or marked with the <see cref="IdentityAttribute"/>
        /// will not be inserted as it's value will be generated by the backing database. If this 
        /// flag is set to <c>False</c>, then every property value not marked with the <see cref="IgnoreAttribute"/>
        /// will be stored into the database.
        /// </param>
        /// <param name="transaction">The transaction</param>
        /// <returns>The number of inserted records.</returns>
        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration")]
        public Task<int> Insert(IEnumerable<T> items, bool modelHasIdentityColumn = true, IDbTransaction transaction = null)
        {
            Ensure.NotNull(items, nameof(items));

            if (!items.Any()) { return _cachedZeroTask; }

            var insertSql = modelHasIdentityColumn ? Table.InsertIdentity : Table.InsertAll;
            return Connection.ExecuteAsync(insertSql, items, transaction: transaction);
        }

        /// <summary>
        /// Inserts every specified columns in the given <paramref name="item"/> as the record.
        /// </summary>
        /// <param name="item">The item to be inserted.</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>The inserted id of the <paramref name="item"/>.</returns>
        public async Task<object> InsertPartial(object item, IDbTransaction transaction = null)
        {
            Ensure.NotNull(item, nameof(item));

            var sql = Table.Dialect.GetPartialInsertQuery<T>(Table, item);
            return (await Connection.QueryAsync<dynamic>(sql, item, transaction: transaction, buffered: true)
                .ConfigureAwait(false)).First().Id;
        }

        /// <summary>
        /// Inserts every specified columns of every item in the given <paramref name="items"/> as the record.
        /// </summary>
        /// <param name="items">The items to be inserted.</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>Number of inserted rows</returns>
        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration")]
        public Task<int> InsertPartial(IEnumerable<object> items, IDbTransaction transaction = null)
        {
            Ensure.NotNull(items, nameof(items));

            if (!items.Any()) { return _cachedZeroTask; }

            var sql = Table.Dialect.GetPartialInsertQuery<T>(Table, items.First());
            return Connection.ExecuteAsync(sql, items, transaction: transaction);
        }

        /// <summary>
        /// Updates every column in the given <paramref name="item"/> (except for the id column) for the record.
        /// <remarks>The id of the <paramref name="item"/> is used to identify the record to be updated.</remarks>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public Task<int> Update(T item, IDbTransaction transaction = null)
            => Connection.ExecuteAsync(Table.UpdateIdentity, item, transaction: transaction);

        /// <summary>
        /// Updates every column for each of the items in the given <paramref name="items"/> (except for the id column) for the record.
        /// <remarks>The id of each item in the <paramref name="items"/> is used to identify the record to be updated.</remarks>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public Task<int> Update(IEnumerable<T> items, IDbTransaction transaction = null)
        {
            Ensure.NotNull(items, nameof(items));
            return Connection.ExecuteAsync(Table.UpdateIdentity, items, transaction: transaction);
        }

        /// <summary>
        /// Updates every column in the given <paramref name="item"/> (including the id column) for the record.
        /// <remarks>The <paramref name="filter"/> is used to identify the record(s) to be updated.</remarks>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public Task<int> UpdateWhere(T item, Filter<T> filter, IDbTransaction transaction = null)
        {
            Ensure.NotNull(filter, nameof(filter));

            var sql = filter.GetSQL(Table.UpdateAll);
            var parameters = filter.Parameters.ToDynamicParameters(item);
            return Connection.ExecuteAsync(sql, parameters, transaction: transaction);
        }

        /// <summary>
        /// Updates every specified columns in the given <paramref name="item"/> for the record.
        /// <remarks>The <paramref name="filter"/> is used to identify the record(s) to be updated.</remarks>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public Task<int> UpdatePartialWhere(object item, Filter<T> filter, IDbTransaction transaction = null)
        {
            Ensure.NotNull(item, nameof(item));
            Ensure.NotNull(filter, nameof(filter));

            var sql = Table.Dialect.GetPartialUpdateQuery(Table, item, filter);
            var parameters = filter.Parameters.ToDynamicParameters();
            parameters.AddDynamicParams(item);
            return Connection.ExecuteAsync(sql, parameters, transaction: transaction);
        }

        /// <summary>
        /// Deletes a record based on the value of the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="value">The value associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>Number of rows affected</returns>
        public Task<int> DeleteWhere<TProperty>(Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var filter = Query<T>.Filter.And(selector, Operator.Equal, value);
            var sql = filter.GetSQL(Table.Delete);
            var parameters = filter.Parameters.ToDynamicParameters();
            return Connection.ExecuteAsync(sql, parameters, transaction: transaction);
        }

        /// <summary>
        /// Deletes a record based on the value of the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <param name="values">The values associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <returns>Number of rows affected</returns>
        public Task<int> DeleteWhere<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values)
        {
            Ensure.NotNull(selector, nameof(selector));
            Ensure.NotNull(values, nameof(values));

            var filter = Query<T>.Filter.AndIn(selector, values);
            var sql = filter.GetSQL(Table.Delete);
            var parameters = filter.Parameters.ToDynamicParameters();
            return Connection.ExecuteAsync(sql, parameters, transaction: transaction);
        }

        /// <summary>
        /// Deletes a record based on the given <paramref name="filter"/>.
        /// </summary>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>Number of rows affected</returns>
        public Task<int> DeleteWhere(Filter<T> filter, IDbTransaction transaction = null)
        {
            Ensure.NotNull(filter, nameof(filter));

            var sql = filter.GetSQL(Table.Delete);
            var parameters = filter.Parameters.ToDynamicParameters();
            return Connection.ExecuteAsync(sql, parameters, transaction: transaction);
        }

        /// <summary>
        /// Deletes all the records.
        /// </summary>
        public Task<int> DeleteAll(IDbTransaction transaction = null) 
            => Connection.ExecuteAsync(Table.Delete, transaction: transaction);

        /// <summary>
        /// Returns the count of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>COUNT</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        public Task<ulong> Count<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = Table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT COUNT ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {Table.Name}";
            return Connection.ExecuteScalarAsync<ulong>(query, transaction: transaction);
        }

        /// <summary>
        /// Returns the count of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>COUNT</c> should be calculated.</param>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        public Task<ulong> Count<TProperty>(Expression<Func<T, TProperty>> selector, Filter<T> filter, bool distinct = false, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));
            Ensure.NotNull(filter, nameof(filter));

            var column = Table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT COUNT ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {Table.Name} WHERE 1=1";

            var sql = filter.GetSQL(query);
            var parameters = filter.Parameters.ToDynamicParameters();
            return Connection.ExecuteScalarAsync<ulong>(sql, parameters, transaction: transaction);
        }

        /// <summary>
        /// Returns the sum of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>SUM</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        public Task<long> Sum<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = Table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT SUM ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {Table.Name}";
            return Connection.ExecuteScalarAsync<long>(query, transaction: transaction);
        }

        /// <summary>
        /// Returns the sum of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>SUM</c> should be calculated.</param>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        public Task<long> Sum<TProperty>(Expression<Func<T, TProperty>> selector, Filter<T> filter, bool distinct = false, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));
            Ensure.NotNull(filter, nameof(filter));

            var column = Table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT SUM ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {Table.Name} WHERE 1=1";

            var sql = filter.GetSQL(query);
            var parameters = filter.Parameters.ToDynamicParameters();
            return Connection.ExecuteScalarAsync<long>(sql, parameters, transaction: transaction);
        }

        /// <summary>
        /// Returns the average of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>AVG</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        public Task<decimal> Avg<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = Table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT AVG ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {Table.Name}";
            return Connection.ExecuteScalarAsync<decimal>(query, transaction: transaction);
        }

        /// <summary>
        /// Returns the average of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>AVG</c> should be calculated.</param>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        public Task<decimal> Avg<TProperty>(Expression<Func<T, TProperty>> selector, Filter<T> filter, bool distinct = false, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));
            Ensure.NotNull(filter, nameof(filter));

            var column = Table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT AVG ({(distinct ? "DISTINCT" : string.Empty)} {column}) FROM {Table.Name} WHERE 1=1";

            var sql = filter.GetSQL(query);
            var parameters = filter.Parameters.ToDynamicParameters();
            return Connection.ExecuteScalarAsync<decimal>(sql, parameters, transaction: transaction);
        }

        /// <summary>
        /// Returns the minimum value defined by the given <paramref name="selector"/>.
        /// <param name="selector">The selector used to identify the column by which the <c>MIN</c> should be calculated.</param>
        /// <param name="transaction">The transaction</param>
        /// </summary>
        public Task<TProperty> Min<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = Table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT MIN ({column}) FROM {Table.Name}";
            return Connection.ExecuteScalarAsync<TProperty>(query, transaction: transaction);
        }

        /// <summary>
        /// Returns the minimum value defined by the given <paramref name="selector"/>.
        /// <param name="selector">The selector used to identify the column by which the <c>MIN</c> should be calculated.</param>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="transaction">The transaction</param>
        /// </summary>
        public Task<TProperty> Min<TProperty>(Expression<Func<T, TProperty>> selector, Filter<T> filter, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));
            Ensure.NotNull(filter, nameof(filter));

            var column = Table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT MIN ({column}) FROM {Table.Name} WHERE 1=1";

            var sql = filter.GetSQL(query);
            var parameters = filter.Parameters.ToDynamicParameters();
            return Connection.ExecuteScalarAsync<TProperty>(sql, parameters, transaction: transaction);
        }

        /// <summary>
        /// Returns the maximum value defined by the given <paramref name="selector"/>.
        /// <param name="selector">The selector used to identify the column by which the <c>MAX</c> should be calculated.</param>
        /// <param name="transaction">The transaction</param>
        /// </summary>
        public Task<TProperty> Max<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));

            var column = Table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT MAX ({column}) FROM {Table.Name}";
            return Connection.ExecuteScalarAsync<TProperty>(query, transaction: transaction);
        }

        /// <summary>
        /// Returns the maximum value defined by the given <paramref name="selector"/>.
        /// <param name="selector">The selector used to identify the column by which the <c>MAX</c> should be calculated.</param>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="transaction">The transaction</param>
        /// </summary>
        public Task<TProperty> Max<TProperty>(Expression<Func<T, TProperty>> selector, Filter<T> filter, IDbTransaction transaction = null)
        {
            Ensure.NotNull(selector, nameof(selector));
            Ensure.NotNull(filter, nameof(filter));

            var column = Table.PropertyNamesToColumns[selector.GetPropertyName()];
            var query = $"SELECT MAX ({column}) FROM {Table.Name} WHERE 1=1";

            var sql = filter.GetSQL(query);
            var parameters = filter.Parameters.ToDynamicParameters();
            return Connection.ExecuteScalarAsync<TProperty>(sql, parameters, transaction: transaction);
        }
    }
}