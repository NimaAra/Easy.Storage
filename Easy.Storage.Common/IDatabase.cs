namespace Easy.Storage.Common
{
    using System;
    using System.Data;
    using System.Threading.Tasks;

    /// <summary>
    /// Specifies the contract required by any implementation of <see cref="IDatabase"/>.
    /// </summary>
    public interface IDatabase : IDisposable
    {
        /// <summary>
        /// Gets the connection.
        /// </summary>
        IDbConnection Connection { get; }

        /// <summary>
        /// Returns <c>True</c> if a table representing <typeparamref name="T"/> exists on the storage.
        /// </summary>
        Task<bool> ExistsAsync<T>();

        /// <summary>
        /// Gets an instance of the <see cref="Repository{T}"/> for the given <typeparamref name="T"/>.
        /// </summary>
        IRepository<T> GetRepository<T>();

        /// <summary>
        /// Gets a new <see cref="IDbTransaction"/> if one isn't already active on the connection.
        /// </summary>
        IDbTransaction BeginTransaction();

        /// <summary>
        /// Gets a new <see cref="IDbTransaction"/> if one isn't already active on the connection.
        /// <remarks>
        /// Unspecified will use the default isolation level specified in the connection string. 
        /// If no isolation level is specified in the connection string, Serializable is used. 
        /// Serializable transactions are the default. In this mode, the engine gets an immediate 
        /// lock on the database, and no other threads may begin a transaction. Other threads may 
        /// read from the database, but not write. With a ReadCommitted isolation level, locks are 
        /// deferred and elevated as needed. It is possible for multiple threads to start a transaction 
        /// in ReadCommitted mode, but if a thread attempts to commit a transaction while another thread 
        /// has a ReadCommitted lock, it may timeout or cause a deadlock on both threads until both threads'
        /// CommandTimeout's are reached. 
        /// </remarks>
        /// </summary>
        /// <param name="isolationLevel">Supported isolation levels are Serializable, ReadCommitted and Unspecified.</param>
        IDbTransaction BeginTransaction(IsolationLevel isolationLevel);
    }
}