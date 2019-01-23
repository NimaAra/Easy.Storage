// ReSharper disable InconsistentNaming
namespace Easy.Storage.SQLite
{
    using System;
    using System.IO;
    using Easy.Common;
    using Easy.Common.Extensions;

    /// <summary>
    /// A helper class to extract data-source of a file-based <c>SQLite</c> database.
    /// </summary>
    internal static class SQLiteHelper
    {
        private const string DataSourceToken = "Data Source=";
        private const string InMemoryToken = ":memory:";
        private static readonly char[] TrimCharacters = {'"'};

        /// <summary>
        /// Gets the database <see cref="FileInfo"/> of a <c>SQLite</c> database created from <paramref name="sqliteConnectionString"/>.
        /// </summary>
        internal static FileInfo GetDatabaseFile(string sqliteConnectionString)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(sqliteConnectionString);
            Ensure.That(sqliteConnectionString.StartsWith(DataSourceToken, StringComparison.OrdinalIgnoreCase), "Invalid SQLite connection string.");
            
            var startIndex = sqliteConnectionString.IndexOf(DataSourceToken, StringComparison.OrdinalIgnoreCase) + DataSourceToken.Length;
            var firstSemicolonIndex = sqliteConnectionString.IndexOf(';', startIndex);

            int endIndex;
            if (firstSemicolonIndex == -1)
            {
                endIndex = sqliteConnectionString.Length - startIndex;
            } else
            {
                endIndex = firstSemicolonIndex - startIndex;
            }

            var dataSourceValue = sqliteConnectionString.Substring(startIndex, endIndex);
            if (dataSourceValue.Equals(InMemoryToken, StringComparison.OrdinalIgnoreCase)) { return null; }

            return new FileInfo(dataSourceValue.Trim(TrimCharacters));
        }

        /// <summary>
        /// Gets the flag indicating whether the given <paramref name="sqliteConnectionString"/> is an in-memory connection.
        /// </summary>
        internal static bool IsInMemoryConnection(string sqliteConnectionString) 
            => sqliteConnectionString.Contains(InMemoryToken, StringComparison.OrdinalIgnoreCase);
    }
}