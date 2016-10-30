namespace Easy.Storage.SqlServer.Extensions
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using Easy.Common;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.SqlServer.Models;

    /// <summary>
    /// Provides a set of methods to help working with <see cref="SqlConnection"/>.
    /// </summary>
    public static class SqlConnectionExtensions
    {
        /// <summary>
        /// Gets an instance of the <see cref="Repository{T}"/> for the given <typeparamref name="T"/>.
        /// </summary>
        public static IRepository<T> GetRepository<T>(this SqlConnection connection)
        {
            return new Repository<T>(connection, Dialect.SqlServer);
        }

        /// <summary>
        /// Returns the <c>SQL Server</c> objects in the database.
        /// </summary>
        public static Task<IEnumerable<SqlServerObject>> GetDatabaseObjectsAsync(this SqlConnection connection)
        {
            return connection.QueryAsync<SqlServerObject>(SqlServerSql.AllObjects);
        }

        /// <summary>
        /// Returns the information relating to the table represented by the <typeparamref name="T"/> in the <c>SQL Server</c> database.
        /// </summary>
        public static Task<SqlServerTableInfo> GetTableInfoAsync<T>(this SqlConnection connection)
        {
            return connection.GetTableInfoAsync(Table.Get<T>().Name);
        }

        /// <summary>
        /// Returns the information relating to the <paramref name="tableName"/>.
        /// </summary>
        public static async Task<SqlServerTableInfo> GetTableInfoAsync(this SqlConnection connection, string tableName)
        {
            Ensure.NotNullOrEmptyOrWhiteSpace(tableName);

            dynamic tableInfo = await connection.QueryAsync<dynamic>(SqlServerSql.TableInfo, new { tableName }).ConfigureAwait(false);

            string database = null, schema = null;
            var counter = 0;

            var columnInfos = new List<SqlServerColumnInfo>();
            foreach (var i in tableInfo)
            {
                if (counter++ == 0)
                {
                    database = i.Database;
                    schema = i.Schema;
                }

                columnInfos.Add(new SqlServerColumnInfo
                {
                    Name = i.Name,
                    Type = ((string)i.TypeName).ParseAsSqlServerDataType(),
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

            return new SqlServerTableInfo
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
            var tableName = Table.Get<T>().Name;
            return await connection.ExecuteScalarAsync<uint>(SqlServerSql.TableExists, new { tableName }).ConfigureAwait(false) != 0;
        }
    }
}