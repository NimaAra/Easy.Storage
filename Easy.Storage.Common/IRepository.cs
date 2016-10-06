namespace Easy.Storage.Common
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq.Expressions;
    using System.Threading.Tasks;

    /// <summary>
    /// Specifies the contract that a repository would need to implement.
    /// </summary>
    public interface IRepository<T> : IDisposable
    {
        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        Task<IEnumerable<T>> GetAsync(IDbTransaction transaction = null);

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        Task<IEnumerable<T>> GetLazyAsync(IDbTransaction transaction = null);

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="value">The value associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        Task<IEnumerable<T>> GetAsync<TProperty>(Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null);

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> from the storage.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <param name="values">The values associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        Task<IEnumerable<T>> GetAsync<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values);

        /// <summary>
        /// Gets the records represented by the <typeparamref name="T"/> based on the given <paramref name="queryFilter"/>.
        /// </summary>
        /// <param name="transaction">The transaction</param>
        /// <param name="queryFilter">The filter to limit the records returned</param>
        Task<IEnumerable<T>> GetAsync(QueryFilter<T> queryFilter, IDbTransaction transaction = null);

        /// <summary>
        /// Inserts the given <paramref name="item"/> to the storage.
        /// </summary>
        /// <returns>The inserted id of the <paramref name="item"/>.</returns>
        Task<long> InsertAsync(T item, IDbTransaction transaction = null);

        /// <summary>
        /// Inserts the given <paramref name="items"/> to the storage.
        /// </summary>
        /// <returns>The number of inserted records.</returns>
        Task<int> InsertAsync(IEnumerable<T> items, IDbTransaction transaction = null);

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the value of the id in the storage.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        Task<int> UpdateAsync(T item, IDbTransaction transaction = null);

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the value of the <paramref name="selector"/>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        Task<int> UpdateAsync<TProperty>(T item, Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null);

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the values of the <paramref name="selector"/>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        Task<int> UpdateAsync<TProperty>(T item, Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values);

        /// <summary>
        /// Updates the given <paramref name="items"/> based on the value of their ids in the storage.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        Task<int> UpdateAsync(IEnumerable<T> items, IDbTransaction transaction = null);

        /// <summary>
        /// Deletes a record based on the value of the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="value">The value associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>Number of rows affected</returns>
        Task<int> DeleteAsync<TProperty>(Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null);

        /// <summary>
        /// Deletes a record based on the value of the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <param name="values">The values associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <returns>Number of rows affected</returns>
        Task<int> DeleteAsync<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values);

        /// <summary>
        /// Deletes all the records.
        /// </summary>
        Task<int> DeleteAllAsync(IDbTransaction transaction = null);

        /// <summary>
        /// Returns the count of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>COUNT</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        Task<ulong> CountAsync<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the sum of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>SUM</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        Task<long> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the average of records based on the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the <c>AVG</c> should be calculated.</param>
        /// <param name="distinct">The flag indicating whether unique values should be considered only or not.</param>
        /// <param name="transaction">The transaction</param>
        Task<decimal> AvgAsync<TProperty>(Expression<Func<T, TProperty>> selector, bool distinct = false, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the minimum value defined by the given <paramref name="selector"/>.
        /// </summary>
        Task<TProperty> MinAsync<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null);

        /// <summary>
        /// Returns the maximum value defined by the given <paramref name="selector"/>.
        /// </summary>
        Task<TProperty> MaxAsync<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null);
    }
}