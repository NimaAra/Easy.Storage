namespace Easy.Storage.Sqlite
{
    using System;
    using System.IO;
    using Easy.Common;

    /// <summary>
    /// A helper class to extract data-source of a file-based <c>SQLite</c> database.
    /// </summary>
    internal static class SqliteHelper
    {
        private const string DataSourceToken = "data source=";
        private const string InMemoryToken = ":memory:";

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

            return new FileInfo(dataSourceValue);
        }
    }
}