namespace Easy.Storage.Tests.Unit.Sqlite
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Easy.Storage.Sqlite;
    using Easy.Storage.Sqlite.Models;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;
    using Easy.Storage.Common.Extensions;

    [TestFixture]
    internal sealed class SqliteDatabaseTests : Context
    {
        [Test]
        public async Task When_getting_sqlite_objects_of_non_aliased_model()
        {
            using (var db = new SqliteDatabase("Data Source=:memory:"))
            {
                var snapshot1 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot1.ShouldNotBeNull();
                snapshot1.ShouldBeEmpty();

                await db.Connection.ExecuteAsync(TableQuery);

                var snapshot2 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot2.ShouldNotBeNull();
                snapshot2.Length.ShouldBe(1);

                snapshot2[0].Type.ShouldBe(SqliteObjectType.Table);
                snapshot2[0].Name.ShouldBe("Person");
                snapshot2[0].Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                await db.Connection.ExecuteAsync(ViewQuery);

                var snapshot3 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot3.ShouldNotBeNull();
                snapshot3.Length.ShouldBe(2);

                snapshot3[0].Type.ShouldBe(SqliteObjectType.Table);
                snapshot3[0].Name.ShouldBe("Person");
                snapshot3[0].Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot3[1].Type.ShouldBe(SqliteObjectType.View);
                snapshot3[1].Name.ShouldBe("Person_view");
                snapshot3[1].Sql.ShouldBe(ViewQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                await db.Connection.ExecuteAsync(TriggerQuery);

                var snapshot4 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot4.ShouldNotBeNull();
                snapshot4.Length.ShouldBe(3);

                snapshot4[0].Type.ShouldBe(SqliteObjectType.Table);
                snapshot4[0].Name.ShouldBe("Person");
                snapshot4[0].Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot4[1].Type.ShouldBe(SqliteObjectType.View);
                snapshot4[1].Name.ShouldBe("Person_view");
                snapshot4[1].Sql.ShouldBe(ViewQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot4[2].Type.ShouldBe(SqliteObjectType.Trigger);
                snapshot4[2].Name.ShouldBe("Person_bu");
                snapshot4[2].Sql.ShouldBe(TriggerQuery.Replace("IF NOT EXISTS ", string.Empty));

                await db.Connection.ExecuteAsync(IndexQuery);

                var snapshot5 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot5.ShouldNotBeNull();
                snapshot5.Length.ShouldBe(4);

                snapshot5[0].Type.ShouldBe(SqliteObjectType.Table);
                snapshot5[0].Name.ShouldBe("Person");
                snapshot5[0].Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot5[1].Type.ShouldBe(SqliteObjectType.View);
                snapshot5[1].Name.ShouldBe("Person_view");
                snapshot5[1].Sql.ShouldBe(ViewQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot5[2].Type.ShouldBe(SqliteObjectType.Trigger);
                snapshot5[2].Name.ShouldBe("Person_bu");
                snapshot5[2].Sql.ShouldBe(TriggerQuery.Replace("IF NOT EXISTS ", string.Empty));

                snapshot5[3].Type.ShouldBe(SqliteObjectType.Index);
                snapshot5[3].Name.ShouldBe("Person_idx");
                snapshot5[3].Sql.ShouldBe(IndexQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));
            }
        }

        [Test]
        public async Task When_getting_sqlite_objects_of_aliased_model()
        {
            using (var db = new SqliteDatabase("Data Source=:memory:"))
            {
                var snapshot1 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot1.ShouldNotBeNull();
                snapshot1.ShouldBeEmpty();

                await db.Connection.ExecuteAsync(TableQuery);

                var snapshot2 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot2.ShouldNotBeNull();
                snapshot2.Length.ShouldBe(1);

                snapshot2[0].Type.ShouldBe(SqliteObjectType.Table);
                snapshot2[0].Name.ShouldBe("Person");
                snapshot2[0].Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                await db.Connection.ExecuteAsync(ViewQuery);

                var snapshot3 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot3.ShouldNotBeNull();
                snapshot3.Length.ShouldBe(2);

                snapshot3[0].Type.ShouldBe(SqliteObjectType.Table);
                snapshot3[0].Name.ShouldBe("Person");
                snapshot3[0].Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot3[1].Type.ShouldBe(SqliteObjectType.View);
                snapshot3[1].Name.ShouldBe("Person_view");
                snapshot3[1].Sql.ShouldBe(ViewQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                await db.Connection.ExecuteAsync(TriggerQuery);

                var snapshot4 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot4.ShouldNotBeNull();
                snapshot4.Length.ShouldBe(3);

                snapshot4[0].Type.ShouldBe(SqliteObjectType.Table);
                snapshot4[0].Name.ShouldBe("Person");
                snapshot4[0].Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot4[1].Type.ShouldBe(SqliteObjectType.View);
                snapshot4[1].Name.ShouldBe("Person_view");
                snapshot4[1].Sql.ShouldBe(ViewQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot4[2].Type.ShouldBe(SqliteObjectType.Trigger);
                snapshot4[2].Name.ShouldBe("Person_bu");
                snapshot4[2].Sql.ShouldBe(TriggerQuery.Replace("IF NOT EXISTS ", string.Empty));

                await db.Connection.ExecuteAsync(IndexQuery);

                var snapshot5 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot5.ShouldNotBeNull();
                snapshot5.Length.ShouldBe(4);

                snapshot5[0].Type.ShouldBe(SqliteObjectType.Table);
                snapshot5[0].Name.ShouldBe("Person");
                snapshot5[0].Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot5[1].Type.ShouldBe(SqliteObjectType.View);
                snapshot5[1].Name.ShouldBe("Person_view");
                snapshot5[1].Sql.ShouldBe(ViewQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot5[2].Type.ShouldBe(SqliteObjectType.Trigger);
                snapshot5[2].Name.ShouldBe("Person_bu");
                snapshot5[2].Sql.ShouldBe(TriggerQuery.Replace("IF NOT EXISTS ", string.Empty));

                snapshot5[3].Type.ShouldBe(SqliteObjectType.Index);
                snapshot5[3].Name.ShouldBe("Person_idx");
                snapshot5[3].Sql.ShouldBe(IndexQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));
            }
        }

        [Test]
        public async Task When_getting_sqlite_table_info_of_non_aliased_model()
        {
            using (var db = new SqliteDatabase("Data Source=:memory:"))
            {
                await db.Connection.ExecuteAsync(TableQuery);
                var tableInfo = await db.GetTableInfoAsync<Person>();

                tableInfo.ShouldNotBeNull();
                tableInfo.TableName.ShouldBe("Person");
                tableInfo.Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));
                tableInfo.Columns.Length.ShouldBe(3);

                Array.TrueForAll(tableInfo.Columns, i => i.TableName == "Person").ShouldBeTrue();

                tableInfo.Columns[0].ColumnId.ShouldBe(0);
                tableInfo.Columns[0].ColumnName.ShouldBe("Id");
                tableInfo.Columns[0].ColumnTpe.ShouldBe(SqliteDataType.INTEGER);
                tableInfo.Columns[0].DefaultValue.ShouldBeNull();
                tableInfo.Columns[0].IsPrimaryKey.ShouldBeTrue();
                tableInfo.Columns[0].NotNull.ShouldBeTrue();

                tableInfo.Columns[1].ColumnId.ShouldBe(1);
                tableInfo.Columns[1].ColumnName.ShouldBe("Name");
                tableInfo.Columns[1].ColumnTpe.ShouldBe(SqliteDataType.TEXT);
                tableInfo.Columns[1].DefaultValue.ShouldBeNull();
                tableInfo.Columns[1].IsPrimaryKey.ShouldBeFalse();
                tableInfo.Columns[1].NotNull.ShouldBeTrue();

                tableInfo.Columns[2].ColumnId.ShouldBe(2);
                tableInfo.Columns[2].ColumnName.ShouldBe("Age");
                tableInfo.Columns[2].ColumnTpe.ShouldBe(SqliteDataType.INTEGER);
                tableInfo.Columns[2].DefaultValue.ShouldBeNull();
                tableInfo.Columns[2].IsPrimaryKey.ShouldBeFalse();
                tableInfo.Columns[2].NotNull.ShouldBeTrue();
            }
        }

        [Test]
        public async Task When_getting_sqlite_table_info_of_aliased_model()
        {
            using (var db = new SqliteDatabase("Data Source=:memory:"))
            {
                await db.Connection.ExecuteAsync(TableQuery);
                var tableInfo = await db.GetTableInfoAsync<MyPerson>();

                tableInfo.ShouldNotBeNull();
                tableInfo.TableName.ShouldBe("Person");
                tableInfo.Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));
                tableInfo.Columns.Length.ShouldBe(3);

                Array.TrueForAll(tableInfo.Columns, i => i.TableName == "Person").ShouldBeTrue();

                tableInfo.Columns[0].ColumnId.ShouldBe(0);
                tableInfo.Columns[0].ColumnName.ShouldBe("Id");
                tableInfo.Columns[0].ColumnTpe.ShouldBe(SqliteDataType.INTEGER);
                tableInfo.Columns[0].DefaultValue.ShouldBeNull();
                tableInfo.Columns[0].IsPrimaryKey.ShouldBeTrue();
                tableInfo.Columns[0].NotNull.ShouldBeTrue();

                tableInfo.Columns[1].ColumnId.ShouldBe(1);
                tableInfo.Columns[1].ColumnName.ShouldBe("Name");
                tableInfo.Columns[1].ColumnTpe.ShouldBe(SqliteDataType.TEXT);
                tableInfo.Columns[1].DefaultValue.ShouldBeNull();
                tableInfo.Columns[1].IsPrimaryKey.ShouldBeFalse();
                tableInfo.Columns[1].NotNull.ShouldBeTrue();

                tableInfo.Columns[2].ColumnId.ShouldBe(2);
                tableInfo.Columns[2].ColumnName.ShouldBe("Age");
                tableInfo.Columns[2].ColumnTpe.ShouldBe(SqliteDataType.INTEGER);
                tableInfo.Columns[2].DefaultValue.ShouldBeNull();
                tableInfo.Columns[2].IsPrimaryKey.ShouldBeFalse();
                tableInfo.Columns[2].NotNull.ShouldBeTrue();
            }
        }

        [Test]
        public void When_getting_sqlite_table_info_for_non_existing_table()
        {
            using (var db = new SqliteDatabase("Data Source=:memory:"))
            {
                var ex = Should.Throw<InvalidOperationException>(() => db.GetTableInfoAsync<Person>());
                ex.Message.ShouldBe("Table: Person does not seem to exist.");
                ex.InnerException.ShouldBeOfType<InvalidOperationException>();
                ex.InnerException?.Message.ShouldBe("No columns were selected");
            }
        }
    }
}