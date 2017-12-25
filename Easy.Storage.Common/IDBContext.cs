namespace Easy.Storage.Common
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Easy.Storage.Common.Filter;

    /// <summary>
    /// Specifies the contract that the storage context would need to implement.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public interface IDBContext<T>
    {
        /// <summary>
        /// Gets the underlying connection.
        /// </summary>
        IDbConnection Connection { get; }
        
        /// <summary>
        /// Gets the abstraction used for working with the model.
        /// </summary>
        Table Table { get; }

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        Task<IEnumerable<T>> Get(IDbTransaction transaction = null);

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        Task<IEnumerable<T>> GetLazy(IDbTransaction transaction = null);

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="value">The value associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        Task<IEnumerable<T>> GetWhere<TProperty>(Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null);

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <param name="values">The values associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        Task<IEnumerable<T>> GetWhere<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values);

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> based on the given <paramref name="filter"/>.
        /// </summary>
        /// <param name="transaction">The transaction</param>
        /// <param name="filter">The filter to limit the records returned</param>
        Task<IEnumerable<T>> GetWhere(Filter<T> filter, IDbTransaction transaction = null);

        /// <summary>
        /// Inserts the given <paramref name="item"/> to the storage.
        /// </summary>
        /// <param name="item">The item to be inserted.</param>
        /// <param name="modelHasIdentityColumn">The flag indicating whether the table has an identity column.</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>The inserted id of the <paramref name="item"/>.</returns>
        Task<dynamic> Insert(T item, bool modelHasIdentityColumn = true, IDbTransaction transaction = null);

        /// <summary>
        /// Inserts the given <paramref name="items"/> to the storage.
        /// </summary>
        /// <param name="items">The items to be inserted.</param>
        /// <param name="modelHasIdentityColumn">The flag indicating whether the table has an identity column.</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>The number of inserted records.</returns>
        Task<int> Insert(IEnumerable<T> items, bool modelHasIdentityColumn = true, IDbTransaction transaction = null);

        /// <summary>
        /// Inserts every specified columns in the given <paramref name="item"/> as the record.
        /// </summary>
        /// <param name="item">The item to be inserted.</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>The inserted id of the <paramref name="item"/>.</returns>
        Task<object> InsertPartial(object item, IDbTransaction transaction = null);

        /// <summary>
        /// Inserts every specified columns of every item in the given <paramref name="items"/> as the record.
        /// </summary>
        /// <param name="items">The items to be inserted.</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>Number of inserted rows</returns>
        Task<int> InsertPartial(IEnumerable<object> items, IDbTransaction transaction = null);

        /// <summary>
        /// Updates every column in the given <paramref name="item"/> (except for the id column) for the record.
        /// <remarks>The id of the <paramref name="item"/> is used to identify the record to be updated.</remarks>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        Task<int> Update(T item, IDbTransaction transaction = null);

        /// <summary>
        /// Updates every column for each of the items in the given <paramref name="items"/> (except for the id column) for the record.
        /// <remarks>The id of each item in the <paramref name="items"/> is used to identify the record to be updated.</remarks>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        Task<int> Update(IEnumerable<T> items, IDbTransaction transaction = null);

        /// <summary>
        /// Updates every column in the given <paramref name="item"/> (including the id column) for the record.
        /// <remarks>The <paramref name="filter"/> is used to identify the record(s) to be updated.</remarks>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        Task<int> UpdateWhere(T item, Filter<T> filter, IDbTransaction transaction = null);

        /// <summary>
        /// Updates every specified columns in the given <paramref name="item"/> for the record.
        /// <remarks>The <paramref name="filter"/> is used to identify the record(s) to be updated.</remarks>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        Task<int> UpdatePartialWhere(object item, Filter<T> filter, IDbTransaction transaction = null);

        /// <summary>
        /// Deletes a record based on the value of the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="value">The value associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>Number of rows affected</returns>
        Task<int> DeleteWhere<TProperty>(Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null);

        /// <summary>
        /// Deletes a record based on the value of the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <param name="values">The values associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <returns>Number of rows affected</returns>
        Task<int> DeleteWhere<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values);

        /// <summary>
        /// Deletes a record based on the given <paramref name="filter"/>.
        /// </summary>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>Number of rows affected</returns>
        Task<int> DeleteWhere(Filter<T> filter, IDbTransaction transaction = null);

        /// <summary>
        /// Deletes all the records.
        /// </summary>
        Task<int> DeleteAll(IDbTransaction transaction = null);

        /// <summary>
        /// Returns the count of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>COUNT</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        Task<ulong> Count<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the count of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>COUNT</c> should be calculated.</param>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        Task<ulong> Count<TProperty>(Expression<Func<T, TProperty>> selector, Filter<T> filter, bool distinct = false, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the sum of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>SUM</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        Task<long> Sum<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the sum of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>SUM</c> should be calculated.</param>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        Task<long> Sum<TProperty>(Expression<Func<T, TProperty>> selector, Filter<T> filter, bool distinct = false, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the average of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>AVG</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        Task<decimal> Avg<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the average of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>AVG</c> should be calculated.</param>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        Task<decimal> Avg<TProperty>(Expression<Func<T, TProperty>> selector, Filter<T> filter, bool distinct = false, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the minimum value defined by the given <paramref name="selector"/>.
        /// <param name="selector">The selector used to identify the column by which the <c>MIN</c> should be calculated.</param>
        /// <param name="transaction">The transaction</param>
        /// </summary>
        Task<TProperty> Min<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the minimum value defined by the given <paramref name="selector"/>.
        /// <param name="selector">The selector used to identify the column by which the <c>MIN</c> should be calculated.</param>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="transaction">The transaction</param>
        /// </summary>
        Task<TProperty> Min<TProperty>(Expression<Func<T, TProperty>> selector, Filter<T> filter, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the maximum value defined by the given <paramref name="selector"/>.
        /// <param name="selector">The selector used to identify the column by which the <c>MAX</c> should be calculated.</param>
        /// <param name="transaction">The transaction</param>
        /// </summary>
        Task<TProperty> Max<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the maximum value defined by the given <paramref name="selector"/>.
        /// <param name="selector">The selector used to identify the column by which the <c>MAX</c> should be calculated.</param>
        /// <param name="filter">The filter to limit the records returned</param>
        /// <param name="transaction">The transaction</param>
        /// </summary>
        Task<TProperty> Max<TProperty>(Expression<Func<T, TProperty>> selector, Filter<T> filter, IDbTransaction transaction = null);
    }
}