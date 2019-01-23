namespace Easy.Storage.SQLite.Connections
{
    using System;
    using System.Data.SQLite;
    using System.IO;
    using System.Text.RegularExpressions;

    /// <summary>
    /// A wrapper around the <see cref="SQLiteConnection"/> for file-based connections.
    /// </summary>
    public sealed class SQLiteFileConnection : SQLiteConnectionBase
    {
        private static readonly Regex _fileRegex = 
            new Regex("(?i)Data Source(?-i)=((?<file>.*?);|(?<file>.*?)$)", RegexOptions.Compiled);
        
        /// <summary>
        /// Creates an instance of the <see cref="SQLiteInMemoryConnection"/>.
        /// </summary>
        public SQLiteFileConnection(FileInfo dbFile)
            : base(SQLiteConnectionStringProvider.GetFileConnectionString(dbFile)) => File = dbFile;

        /// <summary>
        /// Creates an instance of the <see cref="SQLiteInMemoryConnection"/>.
        /// </summary>
        /// <param name="connectionString">A valid <c>SQLite</c> connection-string.</param>
        public SQLiteFileConnection(string connectionString) : base(connectionString)
        {
            if (SQLiteHelper.IsInMemoryConnection(connectionString))
            {
                throw new ArgumentException("Cannot be a SQLite memory connection-string.");
            }

            var match = _fileRegex.Match(connectionString);
            if (!match.Success)
            {
                throw new ArgumentException("Invalid SQLite connection-string.");
            }

            File = new FileInfo(match.Groups["file"].Value);
        }

        public FileInfo File { get; }

        /// <summary>
        /// Disposes and finalizes the connection, if applicable.
        /// </summary>
        public override void Dispose() => Connection.Dispose();
    }
}