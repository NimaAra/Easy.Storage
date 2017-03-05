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
    // ReSharper disable once UnusedMember.Global
    // ReSharper disable once InconsistentNaming
    public static class SQLConnectionExtensions
    {
        /// <summary>
        /// Gets an instance of the <see cref="Repository{T}"/> for the given <typeparamref name="T"/>.
        /// </summary>
        public static IRepository<T> GetRepository<T>(this SqlConnection connection)
        {
            return new Repository<T>(connection, Dialect.SQLServer);
        }

        /// <summary>
        /// Returns the <c>SQL Server</c> objects in the database.
        /// </summary>
        public static Task<IEnumerable<SQLServerObject>> GetDatabaseObjectsAsync(this SqlConnection connection)
        {
            return connection.QueryAsync<SQLServerObject>(SQLServerSQL.AllObjects);
        }

        /// <summary>
        /// Returns the information relating to the table represented by the <typeparamref name="T"/> in the <c>SQL Server</c> database.
        /// </summary>
        public static Task<SQLServerTableInfo> GetTableInfoAsync<T>(this SqlConnection connection)
        {
            return connection.GetTableInfoAsync(Table.Make<T>().Name);
        }

        /// <summary>
        /// Returns the information relating to the <paramref name="tableName"/>.
        /// </summary>
        public static async Task<SQLServerTableInfo> GetTableInfoAsync(this SqlConnection connection, string tableName)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(tableName);

            dynamic tableInfo = await connection.QueryAsync<dynamic>(SQLServerSQL.TableInfo, new { tableName }).ConfigureAwait(false);

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
        public static async Task<bool> ExistsAsync<T>(this SqlConnection connection)
        {
            var tableName = Table.Make<T>().Name;
            return await connection.ExecuteScalarAsync<uint>(SQLServerSQL.TableExists, new { tableName }).ConfigureAwait(false) != 0;
        }
    }
}