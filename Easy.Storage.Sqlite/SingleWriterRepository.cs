namespace Easy.Storage.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Easy.Common;
    using Easy.Storage.Common;

    /// <summary>
    /// Represents a <see cref="Repository{T}"/> which allows a single writer/updater/deleter at a time.
    /// </summary>
    public sealed class SingleWriterRepository<T> : Repository<T>
    {
        private readonly SemaphoreSlim _writerSemaphore;

        /// <summary>
        /// Creates an instance of the <see cref="SingleWriterRepository{T}"/>.
        /// </summary>
        internal SingleWriterRepository(SemaphoreSlim semaphore, IDbConnection connection) : base(connection, Dialect.Sqlite)
        {
            _writerSemaphore = Ensure.NotNull(semaphore, nameof(semaphore));
        }

        /// <summary>
        /// Inserts the given <paramref name="item"/> to the storage.
        /// </summary>
        /// <returns>The inserted id of the <paramref name="item"/>.</returns>
        public override async Task<long> InsertAsync(T item, IDbTransaction transaction = null)
        {
            await _writerSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                RollIfYouShould();
                return await base.InsertAsync(item, transaction).ConfigureAwait(false);
            } finally
            {
                _writerSemaphore.Release();
            }
        }

        /// <summary>
        /// Inserts the given <paramref name="items"/> to the storage.
        /// </summary>
        /// <returns>The number of inserted records.</returns>
        public override async Task<int> InsertAsync(IEnumerable<T> items, IDbTransaction transaction = null)
        {
            await _writerSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                RollIfYouShould();
                return await base.InsertAsync(items, transaction).ConfigureAwait(false);
            } finally
            {
                _writerSemaphore.Release();
            }
        }

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the value of the id in the storage.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public override async Task<int> UpdateAsync(T item, IDbTransaction transaction = null)
        {
            await _writerSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                return await base.UpdateAsync(item, transaction).ConfigureAwait(false);
            } finally
            {
                _writerSemaphore.Release();
            }
        }

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the value of the <paramref name="selector"/>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public override async Task<int> UpdateAsync<TProperty>(T item, Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null)
        {
            await _writerSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                return await base.UpdateAsync(item, selector, value, transaction).ConfigureAwait(false);
            } finally
            {
                _writerSemaphore.Release();
            }
        }

        /// <summary>
        /// Updates the given <paramref name="item"/> based on the values of the <paramref name="selector"/>.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public override async Task<int> UpdateAsync<TProperty>(T item, Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values)
        {
            await _writerSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                return await base.UpdateAsync(item, selector, transaction, values).ConfigureAwait(false);
            } finally
            {
                _writerSemaphore.Release();
            }
        }

        /// <summary>
        /// Updates the given <paramref name="items"/> based on the value of their ids in the storage.
        /// </summary>
        /// <returns>Number of rows affected</returns>
        public override async Task<int> UpdateAsync(IEnumerable<T> items, IDbTransaction transaction = null)
        {
            await _writerSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                return await base.UpdateAsync(items, transaction).ConfigureAwait(false);
            } finally
            {
                _writerSemaphore.Release();
            }
        }

        /// <summary>
        /// Deletes a record based on the value of the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="value">The value associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <returns>Number of rows affected</returns>
        public override async Task<int> DeleteAsync<TProperty>(Expression<Func<T, TProperty>> selector, TProperty value, IDbTransaction transaction = null)
        {
            await _writerSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                return await base.DeleteAsync(selector, value, transaction).ConfigureAwait(false);
            } finally
            {
                _writerSemaphore.Release();
            }
        }

        /// <summary>
        /// Deletes a record based on the value of the given <paramref name="selector"/>.
        /// </summary>
        /// <param name="selector">The selector used to identify the column by which the query should be filtered.</param>
        /// <param name="transaction">The transaction</param>
        /// <param name="values">The values associated to the column specified by the <paramref name="selector"/> by which the query should be filtered.</param>
        /// <returns>Number of rows affected</returns>
        public override async Task<int> DeleteAsync<TProperty>(Expression<Func<T, TProperty>> selector, IDbTransaction transaction = null, params TProperty[] values)
        {
            await _writerSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                return await base.DeleteAsync(selector, transaction, values).ConfigureAwait(false);
            } finally
            {
                _writerSemaphore.Release();
            }
        }

        /// <summary>
        /// Deletes all the records.
        /// </summary>
        public override async Task<int> DeleteAllAsync(IDbTransaction transaction = null)
        {
            await _writerSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                return await base.DeleteAllAsync(transaction).ConfigureAwait(false);
            } finally
            {
                _writerSemaphore.Release();
            }
        }

        private void RollIfYouShould()
        {
            var con = (SqliteConnectionWrapper)Connection;

            if (con.ShouldRoll())
            {
                con.Roll();
            }
        }
    }
}