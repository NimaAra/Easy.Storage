namespace Easy.Storage.SQLite.Extensions
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Easy.Common;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.SQLite.Connections;
    using Easy.Storage.SQLite.FTS;
    using Easy.Storage.SQLite.Models;

    /// <summary>
    /// Provides a set of methods to help working with <see cref="SQLiteConnectionBase"/>.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public static class SQLiteExtensions
    {
        /// <summary>
        /// Gets an instance of the <see cref="IDBContext{T}"/> for the given <typeparamref name="T"/>.
        /// <param name="connection">The database connection.</param>
        /// </summary>
        // ReSharper disable once InconsistentNaming
        public static IDBContext<T> GetDBContext<T>(this SQLiteConnectionBase connection) 
            => new DBContext<T>(connection, SQLiteDialect.Instance);

        /// <summary>
        /// Gets an instance of the <see cref="DBContext{T}"/> for the given <typeparamref name="T"/>.
        /// <param name="connection">The database connection.</param>
        /// <param name="tableName">The name of the table to map the <typeparamref name="T"/> to.</param>
        /// </summary>
        public static IDBContext<T> GetDBContext<T>(
            this SQLiteConnectionBase connection, string tableName)
                => new DBContext<T>(connection, SQLiteDialect.Instance, tableName);

        /// <summary>
        /// Returns the <c>SQLite</c> objects in the database.
        /// </summary>
        public static async Task<IList<SQLiteObject>> GetDatabaseObjects(
            this SQLiteConnectionBase connection) 
                => (await connection.QueryAsync<SQLiteObject>(SQLiteSQL.Master)
                        .ConfigureAwait(false)).SpeculativeToList();

        /// <summary>
        /// Returns <c>True</c> if a table representing <typeparamref name="T"/> exists on the storage.
        /// </summary>
        public static async Task<bool> Exists<T>(this SQLiteConnectionBase connection)
        {
            var tableName = Table.MakeOrGet<T>(SQLiteDialect.Instance, string.Empty)
                .Name.GetNameFromEscapedSQLName();
            return await connection.ExecuteScalarAsync<uint>(SQLiteSQL.TableExists, new { tableName })
                       .ConfigureAwait(false) != 0;
        }

        /// <summary>
        /// Returns <c>True</c> if the given <paramref name="table"/> exists on the storage.
        /// </summary>
        public static async Task<bool> Exists(this SQLiteConnectionBase connection, string table)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(table);
            return await connection.ExecuteScalarAsync<uint>(
                           SQLiteSQL.TableExists, new { tableName = table })
                       .ConfigureAwait(false) != 0;
        }

        /// <summary>
        /// Returns the information relating to the table represented by the <typeparamref name="T"/> in the <c>SQLite</c> database.
        /// </summary>
        public static Task<SQLiteTableInfo> GetTableInfo<T>(this SQLiteConnectionBase connection)
             => connection.GetTableInfo(Table.MakeOrGet<T>(SQLiteDialect.Instance, string.Empty).Name);

        /// <summary>
        /// Returns the information relating to the <paramref name="tableName"/>.
        /// </summary>
        public static async Task<SQLiteTableInfo> GetTableInfo(
            this SQLiteConnectionBase connection, string tableName)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(tableName);

            var escapedTableName = tableName.GetNameFromEscapedSQLName();

            dynamic[] tableInfo = (await connection.QueryAsync<dynamic>(
                $"PRAGMA table_info({escapedTableName})")
                .ConfigureAwait(false)).ToArray();

            if (tableInfo.Length == 0)
            {
                throw new InvalidOperationException($"Table: {tableName} does not exist.");
            }

            var columnsInfo = tableInfo.Select(i =>
            {
                SQLiteDataType columnType;
                string typeStr = i.type.ToString();
                if (typeStr.Equals("INTEGER", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SQLiteDataType.INTEGER;
                }
                else if (typeStr.Equals("REAL", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SQLiteDataType.REAL;
                }
                else if (typeStr.Equals("TEXT", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SQLiteDataType.TEXT;
                }
                else if (typeStr.Equals("BLOB", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SQLiteDataType.BLOB;
                }
                else if (typeStr.Equals("NULL", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SQLiteDataType.NULL;
                }
                else
                {
                    throw new ArgumentOutOfRangeException(nameof(i.type), "Invalid column type of: " + typeStr);
                }

                return new SQLiteColumnInfo
                {
                    TableName = escapedTableName,
                    Id = i.cid,
                    Name = i.name,
                    Type = columnType,
                    NotNull = i.notnull == 1,
                    DefaultValue = i.dflt_value,
                    IsPrimaryKey = i.pk == 1
                };
            }).ToArray();

            var databaseObjects = await connection.GetDatabaseObjects().ConfigureAwait(false);

            return new SQLiteTableInfo
            {
                TableName = escapedTableName,
                SQL = databaseObjects.Single(
                    x => x.Type == SQLiteObjectType.Table && x.Name == escapedTableName).SQL,
                Columns = columnsInfo
            };
        }

        /// <summary>
        /// Returns records matching the given <paramref name="term"/>.
        /// </summary>
        public static Task<IEnumerable<T>> Search<T>(
            this SQLiteConnectionBase connection, ITerm<T> term, bool buffered = true)
        {
            var query = Table.MakeOrGet<T>(SQLiteDialect.Instance, string.Empty)
                .Select.Replace($"{Formatter.Spacer}1 = 1;", $"rowId IN {Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{term}{Formatter.NewLine});");
            return connection.QueryAsync<T>(query, buffered: buffered);
        }

        /// <summary>
        /// Returns records matching the given <paramref name="term"/>.
        /// </summary>
        public static Task<IEnumerable<T>> Search<T>(
            this SQLiteConnectionBase connection, ITerm<T> term, string tableName, bool buffered = true)
        {
            var query = Table.MakeOrGet<T>(SQLiteDialect.Instance, tableName)
                .Select.Replace($"{Formatter.Spacer}1 = 1;", $"rowId IN {Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{term}{Formatter.NewLine});");
            return connection.QueryAsync<T>(query, buffered: buffered);
        }

        /// <summary>
        /// Returns every attached database and its alias.
        /// </summary>
        public static async Task<IDictionary<string, FileInfo>> GetAttachedDatabases(
            this SQLiteConnectionBase connection) 
                => (await connection.QueryAsync<dynamic>(SQLiteSQL.AttachedDatabases).ConfigureAwait(false))
                    .ToDictionary(
                        r => (string)r.name, 
                        r => string.IsNullOrWhiteSpace(r.file) ? null : new FileInfo((string)r.file));
    }
}