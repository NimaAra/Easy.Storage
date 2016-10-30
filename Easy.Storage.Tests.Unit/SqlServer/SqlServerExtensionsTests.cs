﻿namespace Easy.Storage.Tests.Unit.SqlServer
{
    using System;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading.Tasks;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.SqlServer.Extensions;
    using Easy.Storage.SqlServer.Models;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class SqlServerExtensionsTests : Context
    {
        [OneTimeSetUp]
        public void SetUp()
        {
            if (!IsRunningLocaly) { Assert.Ignore("Ignoring SQL Server Tests"); }
        }

        [Test]
        public static async Task When_checking_if_table_exists_non_aliased_models()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                (await conn.ExistsAsync<Person>()).ShouldBeTrue();
            }
        }

        [Test]
        public static async Task When_checking_if_table_exists_aliased_models()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                (await conn.ExistsAsync<MyPerson>()).ShouldBeTrue();
            }
        }

        [Test]
        public async Task When_checking_table_exists()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                (await conn.ExistsAsync<NonExistingTable>()).ShouldBeFalse();
                await conn.ExecuteAsync(TableQuery);
                (await conn.ExistsAsync<Person>()).ShouldBeTrue();
            }
        }

        [Test]
        public async Task When_executing_some_sql()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                (await conn.ExecuteScalarAsync<int>("SELECT 1 FROM sysobjects WHERE name = @name", new {name = "Users"}))
                    .ShouldBe(1);
                (await conn.ExecuteScalarAsync<int>("SELECT 1 FROM sysobjects WHERE name = @name", new { name = "Bambolini" }))
                    .ShouldBe(0);

                var allObjects = await conn.QueryAsync<dynamic>("SELECT * FROM sys.all_objects");
                allObjects.Count().ShouldBeGreaterThan(10);

                var ex = Should.Throw<SqlException>(async () => await conn.ExecuteAsync("SELECT * FROM SomeTable;"));
                ex.Message.ShouldBe("Invalid object name 'SomeTable'.");
                ex.InnerException.ShouldBeNull();
            }
        }

        [Test]
        public async Task When_getting_database_objects()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(TableQuery);
                await conn.ExecuteAsync(ViewQuery);

                var dbObjects = (await conn.GetDatabaseObjectsAsync()).ToArray();

                dbObjects.ShouldNotBeEmpty();
                dbObjects.Length.ShouldBeGreaterThan(2);

                var personTable = dbObjects.Single(o => o.Type == SqlServerObjectType.U && o.Name == "Person");

                dbObjects.ShouldContain(o => o.Type == SqlServerObjectType.PK 
                    && o.ParentId == personTable.Id 
                    && o.SchemaId == personTable.SchemaId
                    && o.CreationDate.Date == personTable.CreationDate.Date
                    && o.CreationDate.Hour == personTable.CreationDate.Hour
                    && o.CreationDate.Minute == personTable.CreationDate.Minute
                    && o.CreationDate.Second == personTable.CreationDate.Second
                    && o.Name.StartsWith("PK__Person__"));

                var personView = dbObjects.Single(o => o.Type == SqlServerObjectType.V && o.Name == "Person_view");

                personView.ParentId.ShouldBe(personTable.ParentId);
                personView.SchemaId.ShouldBe(personTable.SchemaId);
            }
        }

        [Test]
        public async Task When_getting_details_of_a_table()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(TableQuery);

                var tableInfo = await conn.GetTableInfoAsync<Person>();
                tableInfo.ShouldNotBeNull();
                tableInfo.Database.ShouldBe("SandBox");
                tableInfo.Schema.ShouldBe("dbo");
                tableInfo.Name.ShouldBe("Person");

                tableInfo.Columns.Length.ShouldBe(3);
                tableInfo.Columns[0].Name.ShouldBe("Id");
                tableInfo.Columns[0].Type.ShouldBe(SqlServerDataType.Int);
                tableInfo.Columns[0].Precision.ShouldBe((short)10);
                tableInfo.Columns[0].Position.ShouldBe(1);
                tableInfo.Columns[0].MaximumLength.ShouldBe(4);
                tableInfo.Columns[0].Scale.ShouldBe(0);
                tableInfo.Columns[0].IsNullable.ShouldBeFalse();
                tableInfo.Columns[0].Collation.ShouldBeNull();
                tableInfo.Columns[0].IsPrimaryKey.ShouldBeTrue();

                tableInfo.Columns[1].Name.ShouldBe("Name");
                tableInfo.Columns[1].Type.ShouldBe(SqlServerDataType.NVarChar);
                tableInfo.Columns[1].Precision.ShouldBe((short)0);
                tableInfo.Columns[1].Position.ShouldBe(2);
                tableInfo.Columns[1].MaximumLength.ShouldBe(50);
                tableInfo.Columns[1].Scale.ShouldBeNull();
                tableInfo.Columns[1].IsNullable.ShouldBeTrue();
                tableInfo.Columns[1].Collation.ShouldBe("Latin1_General_CS_AS");
                tableInfo.Columns[1].IsPrimaryKey.ShouldBeFalse();

                tableInfo.Columns[2].Name.ShouldBe("Age");
                tableInfo.Columns[2].Type.ShouldBe(SqlServerDataType.Int);
                tableInfo.Columns[2].Precision.ShouldBe((short)10);
                tableInfo.Columns[2].Position.ShouldBe(3);
                tableInfo.Columns[2].MaximumLength.ShouldBe(4);
                tableInfo.Columns[2].Scale.ShouldBe(0);
                tableInfo.Columns[2].IsNullable.ShouldBeFalse();
                tableInfo.Columns[2].Collation.ShouldBeNull();
                tableInfo.Columns[2].IsPrimaryKey.ShouldBeFalse();

                var exception = Should.Throw<InvalidOperationException>(async () => await conn.GetTableInfoAsync("Bambolini"));
                exception.Message.ShouldBe("Table: Bambolini does not seem to exist.");
            }
        }

        // ReSharper disable once ClassNeverInstantiated.Local
        private sealed class NonExistingTable
        {
            // ReSharper disable once UnusedMember.Local
            public long Id { get; set; }
        }
    }
}