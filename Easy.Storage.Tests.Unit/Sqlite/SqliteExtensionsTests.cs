namespace Easy.Storage.Tests.Unit.SQLite
{
    using System;
    using System.Data;
    using System.Data.SQLite;
    using System.Linq;
    using System.Threading.Tasks;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.SQLite;
    using Easy.Storage.SQLite.Connections;
    using Easy.Storage.SQLite.Extensions;
    using Easy.Storage.SQLite.Models;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class SQLiteExtensionsTests : Context
    {
        [Test]
        public async Task When_checking_if_table_exists_non_aliased_models()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                (await conn.Exists<Person>()).ShouldBeFalse();
                await conn.ExecuteAsync(TableQuery);
                (await conn.Exists<Person>()).ShouldBeTrue();
            }
        }

        [Test]
        public async Task When_checking_if_table_exists_aliased_models()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                (await conn.Exists<MyPerson>()).ShouldBeFalse();
                await conn.ExecuteAsync(TableQuery);
                (await conn.Exists<MyPerson>()).ShouldBeTrue();
            }
        }

        [Test]
        public async Task When_getting_objects_of_a_table()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                var snapshot1 = (await conn.GetDatabaseObjects()).ToArray();
                snapshot1.ShouldNotBeNull();
                snapshot1.ShouldBeEmpty();

                await conn.ExecuteAsync(TableQuery);

                var snapshot2 = (await conn.GetDatabaseObjects()).ToArray();
                snapshot2.ShouldNotBeNull();
                snapshot2.Length.ShouldBe(1);

                snapshot2[0].Type.ShouldBe(SQLiteObjectType.Table);
                snapshot2[0].Name.ShouldBe("Person");
                snapshot2[0].SQL.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                await conn.ExecuteAsync(ViewQuery);

                var snapshot3 = (await conn.GetDatabaseObjects()).ToArray();
                snapshot3.ShouldNotBeNull();
                snapshot3.Length.ShouldBe(2);

                snapshot3[0].Type.ShouldBe(SQLiteObjectType.Table);
                snapshot3[0].Name.ShouldBe("Person");
                snapshot3[0].SQL.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot3[1].Type.ShouldBe(SQLiteObjectType.View);
                snapshot3[1].Name.ShouldBe("Person_view");
                snapshot3[1].SQL.ShouldBe(ViewQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                await conn.ExecuteAsync(TriggerQuery);

                var snapshot4 = (await conn.GetDatabaseObjects()).ToArray();
                snapshot4.ShouldNotBeNull();
                snapshot4.Length.ShouldBe(3);

                snapshot4[0].Type.ShouldBe(SQLiteObjectType.Table);
                snapshot4[0].Name.ShouldBe("Person");
                snapshot4[0].SQL.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot4[1].Type.ShouldBe(SQLiteObjectType.View);
                snapshot4[1].Name.ShouldBe("Person_view");
                snapshot4[1].SQL.ShouldBe(ViewQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot4[2].Type.ShouldBe(SQLiteObjectType.Trigger);
                snapshot4[2].Name.ShouldBe("Person_bu");
                snapshot4[2].SQL.ShouldBe(TriggerQuery.Replace("IF NOT EXISTS ", string.Empty));

                await conn.ExecuteAsync(IndexQuery);

                var snapshot5 = (await conn.GetDatabaseObjects()).ToArray();
                snapshot5.ShouldNotBeNull();
                snapshot5.Length.ShouldBe(4);

                snapshot5[0].Type.ShouldBe(SQLiteObjectType.Table);
                snapshot5[0].Name.ShouldBe("Person");
                snapshot5[0].SQL.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot5[1].Type.ShouldBe(SQLiteObjectType.View);
                snapshot5[1].Name.ShouldBe("Person_view");
                snapshot5[1].SQL.ShouldBe(ViewQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot5[2].Type.ShouldBe(SQLiteObjectType.Trigger);
                snapshot5[2].Name.ShouldBe("Person_bu");
                snapshot5[2].SQL.ShouldBe(TriggerQuery.Replace("IF NOT EXISTS ", string.Empty));

                snapshot5[3].Type.ShouldBe(SQLiteObjectType.Index);
                snapshot5[3].Name.ShouldBe("Person_idx");
                snapshot5[3].SQL.ShouldBe(IndexQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));
            }
        }

        [Test]
        public async Task When_getting_table_info_of_non_aliased_model()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await conn.ExecuteAsync(TableQuery);
                var tableInfo = await conn.GetTableInfo<Person>();

                tableInfo.ShouldNotBeNull();
                tableInfo.TableName.ShouldBe("Person");
                tableInfo.SQL.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));
                tableInfo.Columns.Length.ShouldBe(3);

                Array.TrueForAll(tableInfo.Columns, i => i.TableName == "Person").ShouldBeTrue();

                tableInfo.Columns[0].Id.ShouldBe(0);
                tableInfo.Columns[0].Name.ShouldBe("Id");
                tableInfo.Columns[0].Type.ShouldBe(SQLiteDataType.INTEGER);
                tableInfo.Columns[0].DefaultValue.ShouldBeNull();
                tableInfo.Columns[0].IsPrimaryKey.ShouldBeTrue();
                tableInfo.Columns[0].NotNull.ShouldBeTrue();

                tableInfo.Columns[1].Id.ShouldBe(1);
                tableInfo.Columns[1].Name.ShouldBe("Name");
                tableInfo.Columns[1].Type.ShouldBe(SQLiteDataType.TEXT);
                tableInfo.Columns[1].DefaultValue.ShouldBeNull();
                tableInfo.Columns[1].IsPrimaryKey.ShouldBeFalse();
                tableInfo.Columns[1].NotNull.ShouldBeTrue();

                tableInfo.Columns[2].Id.ShouldBe(2);
                tableInfo.Columns[2].Name.ShouldBe("Age");
                tableInfo.Columns[2].Type.ShouldBe(SQLiteDataType.INTEGER);
                tableInfo.Columns[2].DefaultValue.ShouldBeNull();
                tableInfo.Columns[2].IsPrimaryKey.ShouldBeFalse();
                tableInfo.Columns[2].NotNull.ShouldBeTrue();
            }
        }

        [Test]
        public async Task When_getting_table_info_of_aliased_model()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await conn.ExecuteAsync(TableQuery);
                var tableInfo = await conn.GetTableInfo<MyPerson>();

                tableInfo.ShouldNotBeNull();
                tableInfo.TableName.ShouldBe("Person");
                tableInfo.SQL.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));
                tableInfo.Columns.Length.ShouldBe(3);

                Array.TrueForAll(tableInfo.Columns, i => i.TableName == "Person").ShouldBeTrue();

                tableInfo.Columns[0].Id.ShouldBe(0);
                tableInfo.Columns[0].Name.ShouldBe("Id");
                tableInfo.Columns[0].Type.ShouldBe(SQLiteDataType.INTEGER);
                tableInfo.Columns[0].DefaultValue.ShouldBeNull();
                tableInfo.Columns[0].IsPrimaryKey.ShouldBeTrue();
                tableInfo.Columns[0].NotNull.ShouldBeTrue();

                tableInfo.Columns[1].Id.ShouldBe(1);
                tableInfo.Columns[1].Name.ShouldBe("Name");
                tableInfo.Columns[1].Type.ShouldBe(SQLiteDataType.TEXT);
                tableInfo.Columns[1].DefaultValue.ShouldBeNull();
                tableInfo.Columns[1].IsPrimaryKey.ShouldBeFalse();
                tableInfo.Columns[1].NotNull.ShouldBeTrue();

                tableInfo.Columns[2].Id.ShouldBe(2);
                tableInfo.Columns[2].Name.ShouldBe("Age");
                tableInfo.Columns[2].Type.ShouldBe(SQLiteDataType.INTEGER);
                tableInfo.Columns[2].DefaultValue.ShouldBeNull();
                tableInfo.Columns[2].IsPrimaryKey.ShouldBeFalse();
                tableInfo.Columns[2].NotNull.ShouldBeTrue();
            }
        }

        [Test]
        public void When_getting_table_info_of_a_non_existing_table()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                var ex1 = Should.Throw<InvalidOperationException>(() => conn.GetTableInfo<Person>());
                ex1.Message.ShouldBe("Table: [Person] does not exist.");
                ex1.InnerException.ShouldBeOfType<InvalidOperationException>();
                ex1.InnerException?.Message.ShouldBe("No columns were selected");

                var ex2 = Should.Throw<InvalidOperationException>(() => conn.GetTableInfo("Bambolini"));
                ex2.Message.ShouldBe("Table: Bambolini does not exist.");
                ex2.InnerException.ShouldBeOfType<InvalidOperationException>();
                ex2.InnerException?.Message.ShouldBe("No columns were selected");
            }
        }

        [Test]
        public async Task When_checking_table_exists()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                (await conn.Exists<Person>()).ShouldBeFalse();
                conn.State.ShouldBe(ConnectionState.Open);
                await conn.ExecuteAsync(TableQuery);
                (await conn.Exists<Person>()).ShouldBeTrue();
            }
        }

        [Test]
        public async Task When_executing_some_sql()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await conn.ExecuteAsync(TableQuery);
                (await conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Person")).ShouldBe(0);

                var allRows = await conn.QueryAsync<dynamic>("SELECT * FROM Person");
                allRows.ShouldBeEmpty();

                var ex = Should.Throw<SQLiteException>(async () =>  await conn.ExecuteAsync("SELECT * FROM SomeTable;"));
                ex.Message.ShouldBe("SQL logic error or missing database\r\nno such table: SomeTable");
                ex.InnerException.ShouldBeNull();
            }
        }

        [Test]
        public async Task When_querying_multiple_single_rows()
        {
            using (var conn = new SQLiteInMemoryConnection())
            using(var reader = await conn.QueryMultipleAsync("SELECT 'Foo'; SELECT 666; SELECT 3.43;"))
            {
                reader.IsConsumed.ShouldBeFalse();

                var first = await reader.ReadFirstOrDefaultAsync<string>();
                reader.IsConsumed.ShouldBeFalse();
                first.ShouldBe("Foo");

                var second = await reader.ReadFirstOrDefaultAsync<dynamic>();
                reader.IsConsumed.ShouldBeFalse();
                second.ShouldBe(100);

                var three = await reader.ReadFirstOrDefaultAsync<double>();
                reader.IsConsumed.ShouldBeTrue();
                three.ShouldBe(3.43);
            }
        }

        [Test]
        public async Task When_querying_multiple_multiple_rows()
        {
            using (var conn = new SQLiteInMemoryConnection())
            {
                await conn.ExecuteAsync(SQLiteSQLGenerator.Table<ModelOne>());
                await conn.ExecuteAsync(SQLiteSQLGenerator.Table<ModelTwo>());

                var repoOne = conn.GetRepository<ModelOne>(Dialect.SQLite);
                var repoTwo = conn.GetRepository<ModelTwo>(Dialect.SQLite);

                await repoOne.Insert(new[]
                {
                    new ModelOne {Name = "M1-A"},
                    new ModelOne {Name = "M1-B"},
                    new ModelOne {Name = "M1-C"}
                });

                await repoTwo.Insert(new[]
                {
                    new ModelTwo {Category = "M2-C-A", Number = 1},
                    new ModelTwo {Category = "M2-C-B", Number = 2},
                    new ModelTwo {Category = "M2-C-C", Number = 3},
                    new ModelTwo {Category = "M2-C-D", Number = 4}
                });

                using (var reader = await conn.QueryMultipleAsync("SELECT Id, Name FROM ModelOne; SELECT Id, Number, Category FROM ModelTwo;"))
                {
                    reader.IsConsumed.ShouldBeFalse();

                    var modelOnes = (await reader.ReadAsync<ModelOne>(false)).ToArray();
                    reader.IsConsumed.ShouldBeFalse();
                    
                    modelOnes.Length.ShouldBe(3);
                    modelOnes[0].Name.ShouldBe("M1-A");
                    modelOnes[1].Name.ShouldBe("M1-B");
                    modelOnes[2].Name.ShouldBe("M1-C");

                    var modelTwos = (await reader.ReadAsync<ModelTwo>()).ToArray();
                    reader.IsConsumed.ShouldBeTrue();

                    modelTwos.Length.ShouldBe(4);
                    modelTwos[0].Number.ShouldBe(1);
                    modelTwos[0].Category.ShouldBe("M2-C-A");

                    modelTwos[1].Number.ShouldBe(2);
                    modelTwos[1].Category.ShouldBe("M2-C-B");

                    modelTwos[2].Number.ShouldBe(3);
                    modelTwos[2].Category.ShouldBe("M2-C-C");

                    modelTwos[3].Number.ShouldBe(4);
                    modelTwos[3].Category.ShouldBe("M2-C-D");
                }
            }
        }

        private sealed class ModelOne
        {
            public long Id { get; set; }
            public string Name { get; set; }
        }

        private sealed class ModelTwo
        {
            public long Id { get; set; }
            public int Number { get; set; }
            public string Category { get; set; }
        }
    }
}