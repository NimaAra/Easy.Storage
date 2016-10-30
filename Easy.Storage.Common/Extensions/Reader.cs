namespace Easy.Storage.Common.Extensions
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Dapper;

    /// <summary>
    /// An abstraction over the <c>Dapper GridReader</c>.
    /// </summary>
    public sealed class Reader : IDisposable
    {
        private readonly SqlMapper.GridReader _reader;

        internal Reader(SqlMapper.GridReader reader)
        {
            _reader = reader;
        }

        /// <summary>
        /// Gets the flag indicating whether the underlying reader has been consumed.
        /// </summary>
        public bool IsConsumed => _reader.IsConsumed;

        /// <summary>
        /// Reads the next set of result.
        /// </summary>
        public Task<IEnumerable<T>> ReadAsync<T>(bool buffered = true)
        {
            return _reader.ReadAsync<T>(buffered);
        }

        /// <summary>
        /// Reads an individual row of the next rest of result.
        /// </summary>
        public Task<T> ReadFirstOrDefaultAsync<T>()
        {
            return _reader.ReadFirstOrDefaultAsync<T>();
        }

        /// <summary>
        /// Disposes and finalizes the connection, if applicable.
        /// </summary>
        public void Dispose()
        {
            _reader.Dispose();
        }
    }
}