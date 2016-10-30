namespace Easy.Storage.Sqlite.Extensions
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Easy.Common;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.Sqlite.Connections;
    using Easy.Storage.Sqlite.FTS;
    using Easy.Storage.Sqlite.Models;

    /// <summary>
    /// Provides a set of methods to help working with <see cref="SqliteConnectionBase"/>.
    /// </summary>
    public static class SqliteExtensions
    {
        /// <summary>
        /// Gets an instance of the <see cref="Repository{T}"/> for the given <typeparamref name="T"/>.
        /// </summary>
        public static IRepository<T> GetRepository<T>(this SqliteConnectionBase connection)
        {
            return new Repository<T>(connection, Dialect.Sqlite);
        }

        /// <summary>
        /// Returns the <c>SQLite</c> objects in the database.
        /// </summary>
        public static Task<IEnumerable<SqliteObject>> GetDatabaseObjectsAsync(this SqliteConnectionBase connection)
        {
            return connection.QueryAsync<SqliteObject>(SqliteSql.Master);
        }

        /// <summary>
        /// Returns <c>True</c> if a table representing <typeparamref name="T"/> exists on the storage.
        /// </summary>
        public static async Task<bool> ExistsAsync<T>(this SqliteConnectionBase connection)
        {
            var tableName = Table.Get<T>().Name;
            return await connection.ExecuteScalarAsync<uint>(SqliteSql.TableExists, new { tableName }).ConfigureAwait(false) != 0;
        }

        /// <summary>
        /// Returns the information relating to the table represented by the <typeparamref name="T"/> in the <c>SQLite</c> database.
        /// </summary>
        public static Task<SqliteTableInfo> GetTableInfoAsync<T>(this SqliteConnectionBase connection)
        {
            return connection.GetTableInfoAsync(Table.Get<T>().Name);
        }

        /// <summary>
        /// Returns the information relating to the <paramref name="tableName"/>.
        /// </summary>
        public static async Task<SqliteTableInfo> GetTableInfoAsync(this SqliteConnectionBase connection, string tableName)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(tableName);

            IEnumerable<dynamic> tableInfo;
            try
            {
                tableInfo = await connection.QueryAsync<dynamic>($"PRAGMA table_info({tableName})").ConfigureAwait(false);
            }
            catch (InvalidOperationException e)
            {
                throw new InvalidOperationException($"Table: {tableName} does not exist.", e);
            }

            var columnsInfo = tableInfo.Select(i =>
            {
                SqliteDataType columnType;
                string typeStr = i.type.ToString();
                if (typeStr.Equals("INTEGER", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SqliteDataType.INTEGER;
                }
                else if (typeStr.Equals("REAL", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SqliteDataType.REAL;
                }
                else if (typeStr.Equals("TEXT", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SqliteDataType.TEXT;
                }
                else if (typeStr.Equals("BLOB", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SqliteDataType.BLOB;
                }
                else if (typeStr.Equals("NULL", StringComparison.OrdinalIgnoreCase))
                {
                    columnType = SqliteDataType.NULL;
                }
                else
                {
                    throw new ArgumentOutOfRangeException(nameof(i.type), "Invalid column type of: " + typeStr);
                }

                return new SqliteColumnInfo
                {
                    TableName = tableName,
                    Id = i.cid,
                    Name = i.name,
                    Type = columnType,
                    NotNull = i.notnull == 1,
                    DefaultValue = i.dflt_value,
                    IsPrimaryKey = i.pk == 1
                };
            }).ToArray();

            var databaseObjects = await connection.GetDatabaseObjectsAsync();

            return new SqliteTableInfo
            {
                TableName = tableName,
                Sql = databaseObjects.Single(x => x.Type == SqliteObjectType.Table && x.Name == tableName).Sql,
                Columns = columnsInfo
            };
        }

        /// <summary>
        /// Returns records matching the given <paramref name="term"/>.
        /// </summary>
        public static Task<IEnumerable<T>> SearchAsync<T>(this SqliteConnectionBase connection, ITerm<T> term, bool buffered = true)
        {
            var query = Table.Get<T>().Select.Replace($"{Formatter.Spacer}1 = 1;", $"rowId IN {Formatter.NewLine}({Formatter.NewLine}{Formatter.Spacer}{term}{Formatter.NewLine});");
            return connection.QueryAsync<T>(query, buffered: buffered);
        }

        /// <summary>
        /// Returns every attached database and its alias.
        /// </summary>
        public static async Task<IDictionary<string, FileInfo>> GetAttachedDatabasesAsync(this SqliteConnectionBase connection)
        {
            return (await connection.QueryAsync<dynamic>(SqliteSql.AttachedDatabases))
                        .ToDictionary(r => (string)r.name, r => string.IsNullOrWhiteSpace(r.file) ? null : new FileInfo((string)r.file));
        }
    }
}