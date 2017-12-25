namespace Easy.Storage.SQLServer.Extensions
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using Easy.Common;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.SQLServer.Models;

    /// <summary>
    /// Provides a set of methods to help working with <see cref="SqlConnection"/>.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public static class SQLConnectionExtensions
    {
        /// <summary>
        /// Gets an instance of the <see cref="IDBContext{T}"/> for the given <typeparamref name="T"/>.
        /// <param name="connection">The database connection.</param>
        /// </summary>
        // ReSharper disable once InconsistentNaming
        public static IDBContext<T> GetDBContext<T>(this SqlConnection connection) 
            => new DBContext<T>(connection, SQLServerDialect.Instance);

        /// <summary>
        /// Returns the <c>SQL Server</c> objects in the database.
        /// </summary>
        public static Task<IEnumerable<SQLServerObject>> GetDatabaseObjects(
            this SqlConnection connection) 
                => connection.QueryAsync<SQLServerObject>(SQLServerSQL.AllObjects);

        /// <summary>
        /// Returns the information relating to the table represented by the <typeparamref name="T"/> in the <c>SQL Server</c> database.
        /// </summary>
        public static Task<SQLServerTableInfo> GetTableInfo<T>(this SqlConnection connection)
            => connection.GetTableInfo(Table.MakeOrGet<T>(SQLServerDialect.Instance).Name.GetNameFromEscapedSQLName());

        /// <summary>
        /// Returns the information relating to the <paramref name="tableName"/>.
        /// </summary>
        public static async Task<SQLServerTableInfo> GetTableInfo(this SqlConnection connection, string tableName)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(tableName);

            dynamic tableInfo = await connection.QueryAsync<dynamic>(
                SQLServerSQL.TableInfo, new { tableName })
                .ConfigureAwait(false);

            string database = null, schema = null;
            var counter = 0;

            var columnInfos = new List<SQLServerColumnInfo>();
            foreach (var i in tableInfo)
            {
                if (counter++ == 0)
                {
                    database = i.Database;
                    schema = i.Schema;
                }

                columnInfos.Add(new SQLServerColumnInfo
                {
                    Name = i.Name,
                    Type = ((string)i.TypeName).ParseAsSQLServerDataType(),
                    Precision = i.Precision,
                    Position = i.Position,
                    MaximumLength = i.MaximumLength,
                    Scale = i.Scale,
                    IsNullable = i.IsNullable == "YES",
                    Collation = i.Collation,
                    IsPrimaryKey = i.IsPrimaryKey == 1
                });
            }

            if (counter == 0)
            {
                throw new InvalidOperationException($"Table: {tableName} does not seem to exist.");
            }

            return new SQLServerTableInfo
            {
                Database = database,
                Schema = schema,
                Name = tableName,
                Columns = columnInfos.ToArray()
            };
        }

        /// <summary>
        /// Returns <c>True</c> if a table representing <typeparamref name="T"/> exists on the storage.
        /// </summary>
        public static async Task<bool> Exists<T>(this SqlConnection connection)
        {
            var tableName = Table.MakeOrGet<T>(SQLServerDialect.Instance).Name.GetNameFromEscapedSQLName();
            return await connection.ExecuteScalarAsync<uint>(
                       SQLServerSQL.TableExists, new { tableName })
                       .ConfigureAwait(false) != 0;
        }
    }
}