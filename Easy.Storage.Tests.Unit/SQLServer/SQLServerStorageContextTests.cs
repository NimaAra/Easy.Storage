namespace Easy.Storage.Tests.Unit.SQLServer
{
    using System;
    using System.Data.SqlClient;
    using System.Dynamic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Easy.Storage.Common;
    using Easy.Storage.Common.Attributes;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.Common.Filter;
    using Easy.Storage.SQLServer;
    using Easy.Storage.SQLServer.Extensions;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class SQLServerStorageContextTests : Context
    {
        [OneTimeSetUp]
        public void SetUp()
        {
            if (!IsRunningLocaly) { Assert.Ignore("Ignoring SQL Server Tests"); }
        }

        [Test]
        public async Task Run()
        {
            When_checking_table_non_aliased_model();
            When_checking_table_aliased_model();
            When_checking_overridden_table_model();
            await When_getting_non_aliased_models_lazily();
            await When_getting_aliased_models_lazily();
            await When_getting_non_aliased_models();
            await When_getting_aliased_models();
            await When_getting_non_aliased_models_by_selector();
            await When_getting_aliased_models_by_selector();
            await When_getting_non_aliased_models_by_filter();
            await When_getting_aliased_models_by_filter();
            await When_inserting_single_non_aliased_model();
            await When_inserting_single_aliased_model();
            await When_inserting_multiple_non_aliased_model();
            await When_inserting_multiple_aliased_model();
            await When_inserting_model_with_no_identity_column();
            await When_inserting_model_with_string_identity_column();
            await When_inserting_model_with_guid_identity_column();
            await When_inserting_partial_non_aliased_model();
            await When_inserting_partial_aliased_model();
            await When_inserting_multiple_partial_non_aliased_models();
            await When_inserting_multiple_partial_aliased_models();
            await When_updating_non_aliased_model();
            await When_updating_aliased_model();
            await When_updating_non_aliased_model_with_filter();
            await When_updating_aliased_model_with_filter();
            await When_partially_updating_non_aliased_model();
            await When_partially_updating_aliased_model();
            await When_updating_single_by_id_non_aliased_model();
            await When_updating_single_by_id_aliased_model();
            await When_updating_custom_non_aliased_model();
            await When_updating_custom_aliased_model();
            await When_updating_multiple_non_aliased_models();
            await When_updating_multiple_aliased_models();
            await When_deleting_non_aliased_model();
            await When_deleting_aliased_model();
            await When_deleting_all_non_aliased_model();
            await When_deleting_all_aliased_model();
            await When_counting_non_aliased_model();
            await When_counting_aliased_model();
            await When_min_non_aliased_model();
            await When_min_aliased_model();
            await When_max_non_aliased_model();
            await When_max_aliased_model();
            await When_sum_non_aliased_model();
            await When_sum_aliased_model();
            await When_avg_non_aliased_model();
            await When_avg_aliased_model();
            await When_doing_multiple_operations_with_sample_model();
            await When_working_with_inheritted_model();

            await SQLServerStorageContextTranscationTests.Run();
        }

        private static void When_checking_table_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>();
                var table = repo.Table;
                table.Dialect.ShouldBe(SQLServerDialect.Instance);
                table.Dialect.Type.ShouldBe(DialectType.SQLServer);
                table.Name.ShouldBe("[Person]");

                table.Select.ShouldBe("SELECT\r\n"
                        + "    [Person].[Id] AS 'Id',\r\n"
                        + "    [Person].[Name] AS 'Name',\r\n"
                        + "    [Person].[Age] AS 'Age'\r\n"
                        + "FROM [Person]\r\nWHERE\r\n    1 = 1;");

                table.InsertIdentity.ShouldBe("DECLARE @InsertedRows AS TABLE (Id BIGINT);\r\n"
                    + "INSERT INTO [Person]\r\n"
                        + "(\r\n"
                        + "    [Name],\r\n"
                        + "    [Age]\r\n"
                        + ") OUTPUT Inserted.[Id] INTO @InsertedRows\r\n"
                        + "VALUES\r\n"
                        + "(\r\n"
                        + "    @Name,\r\n"
                        + "    @Age\r\n"
                        + ");\r\n"
                        + "SELECT Id FROM @InsertedRows;");

                table.UpdateIdentity.ShouldBe("UPDATE [Person] SET\r\n"
                        + "    [Name] = @Name,\r\n"
                        + "    [Age] = @Age\r\n"
                        + "WHERE\r\n    [Id] = @Id;");

                table.UpdateAll.ShouldBe("UPDATE [Person] SET\r\n"
                        + "    [Id] = @Id,\r\n"
                        + "    [Name] = @Name,\r\n"
                        + "    [Age] = @Age\r\n"
                        + "WHERE\r\n    1 = 1;");

                table.Delete.ShouldBe("DELETE FROM [Person]\r\nWHERE\r\n    1 = 1;");
            }
        }

        private static void When_checking_table_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);
                var table = repo.Table;
                table.Dialect.ShouldBe(SQLServerDialect.Instance);
                table.Dialect.Type.ShouldBe(DialectType.SQLServer);
                table.Name.ShouldBe("[Person]");

                table.Select.ShouldBe("SELECT\r\n"
                                      + "    [Person].[Id] AS 'SomeId',\r\n"
                                      + "    [Person].[Name] AS 'SomeName',\r\n"
                                      + "    [Person].[Age] AS 'Age'\r\n"
                                      + "FROM [Person]\r\nWHERE\r\n    1 = 1;");

                table.InsertIdentity.ShouldBe("DECLARE @InsertedRows AS TABLE (Id BIGINT);\r\n"
                                              + "INSERT INTO [Person]\r\n"
                                              + "(\r\n"
                                              + "    [Name],\r\n"
                                              + "    [Age]\r\n"
                                              + ") OUTPUT Inserted.[Id] INTO @InsertedRows\r\n"
                                              + "VALUES\r\n"
                                              + "(\r\n"
                                              + "    @SomeName,\r\n"
                                              + "    @Age\r\n"
                                              + ");\r\n"
                                              + "SELECT Id FROM @InsertedRows;");

                table.UpdateIdentity.ShouldBe("UPDATE [Person] SET\r\n"
                                             + "    [Name] = @SomeName,\r\n"
                                             + "    [Age] = @Age\r\n"
                                             + "WHERE\r\n    [Id] = @SomeId;");

                table.UpdateAll.ShouldBe("UPDATE [Person] SET\r\n"
                                            + "    [Id] = @SomeId,\r\n"
                                            + "    [Name] = @SomeName,\r\n"
                                            + "    [Age] = @Age\r\n"
                                            + "WHERE\r\n    1 = 1;");

                table.Delete.ShouldBe("DELETE FROM [Person]\r\nWHERE\r\n    1 = 1;");
            }
        }

        private static void When_checking_overridden_table_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<AnotherPerson>("Person");
                var table = repo.Table;
                table.Dialect.ShouldBe(SQLServerDialect.Instance);
                table.Dialect.Type.ShouldBe(DialectType.SQLServer);
                table.Name.ShouldBe("[Person]");

                table.Select.ShouldBe("SELECT\r\n"
                                      + "    [Person].[Id] AS 'SomeId',\r\n"
                                      + "    [Person].[Name] AS 'SomeName',\r\n"
                                      + "    [Person].[Age] AS 'Age'\r\n"
                                      + "FROM [Person]\r\nWHERE\r\n    1 = 1;");

                table.InsertIdentity.ShouldBe("DECLARE @InsertedRows AS TABLE (Id BIGINT);\r\n"
                                              + "INSERT INTO [Person]\r\n"
                                              + "(\r\n"
                                              + "    [Name],\r\n"
                                              + "    [Age]\r\n"
                                              + ") OUTPUT Inserted.[Id] INTO @InsertedRows\r\n"
                                              + "VALUES\r\n"
                                              + "(\r\n"
                                              + "    @SomeName,\r\n"
                                              + "    @Age\r\n"
                                              + ");\r\n"
                                              + "SELECT Id FROM @InsertedRows;");

                table.UpdateIdentity.ShouldBe("UPDATE [Person] SET\r\n"
                                             + "    [Name] = @SomeName,\r\n"
                                             + "    [Age] = @Age\r\n"
                                             + "WHERE\r\n    [Id] = @SomeId;");

                table.UpdateAll.ShouldBe("UPDATE [Person] SET\r\n"
                                            + "    [Id] = @SomeId,\r\n"
                                            + "    [Name] = @SomeName,\r\n"
                                            + "    [Age] = @Age\r\n"
                                            + "WHERE\r\n    1 = 1;");

                table.Delete.ShouldBe("DELETE FROM [Person]\r\nWHERE\r\n    1 = 1;");
            }
        }

        private static async Task When_getting_non_aliased_models_lazily()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>();

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.GetLazy()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 40 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                var insertedPeopleBuffered = (await repo.GetLazy()).ToArray();
                var insertedPeopleUnBuffered = (await repo.GetLazy()).ToArray();

                insertedPeopleBuffered.Length.ShouldBe(people.Length);
                insertedPeopleUnBuffered.Length.ShouldBe(people.Length);

                insertedPeopleBuffered[0].Id.ShouldBe(1);
                insertedPeopleBuffered[0].Name.ShouldBe("P1");
                insertedPeopleBuffered[0].Age.ShouldBe(10);

                insertedPeopleBuffered[1].Id.ShouldBe(2);
                insertedPeopleBuffered[1].Name.ShouldBe("P2");
                insertedPeopleBuffered[1].Age.ShouldBe(20);

                insertedPeopleBuffered[2].Id.ShouldBe(3);
                insertedPeopleBuffered[2].Name.ShouldBe("P3");
                insertedPeopleBuffered[2].Age.ShouldBe(30);

                insertedPeopleBuffered[3].Id.ShouldBe(4);
                insertedPeopleBuffered[3].Name.ShouldBe("P4");
                insertedPeopleBuffered[3].Age.ShouldBe(40);

                insertedPeopleUnBuffered[0].Id.ShouldBe(1);
                insertedPeopleUnBuffered[0].Name.ShouldBe("P1");
                insertedPeopleUnBuffered[0].Age.ShouldBe(10);

                insertedPeopleUnBuffered[1].Id.ShouldBe(2);
                insertedPeopleUnBuffered[1].Name.ShouldBe("P2");
                insertedPeopleUnBuffered[1].Age.ShouldBe(20);

                insertedPeopleUnBuffered[2].Id.ShouldBe(3);
                insertedPeopleUnBuffered[2].Name.ShouldBe("P3");
                insertedPeopleUnBuffered[2].Age.ShouldBe(30);

                insertedPeopleUnBuffered[3].Id.ShouldBe(4);
                insertedPeopleUnBuffered[3].Name.ShouldBe("P4");
                insertedPeopleUnBuffered[3].Age.ShouldBe(40);
            }
        }

        private static async Task When_getting_aliased_models_lazily()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.GetLazy()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 40 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                var insertedPeopleBuffered = (await repo.GetLazy()).ToArray();
                var insertedPeopleUnBuffered = (await repo.GetLazy()).ToArray();

                insertedPeopleBuffered.Length.ShouldBe(people.Length);
                insertedPeopleUnBuffered.Length.ShouldBe(people.Length);

                insertedPeopleBuffered[0].SomeId.ShouldBe(1);
                insertedPeopleBuffered[0].SomeName.ShouldBe("P1");
                insertedPeopleBuffered[0].Age.ShouldBe(10);

                insertedPeopleBuffered[1].SomeId.ShouldBe(2);
                insertedPeopleBuffered[1].SomeName.ShouldBe("P2");
                insertedPeopleBuffered[1].Age.ShouldBe(20);

                insertedPeopleBuffered[2].SomeId.ShouldBe(3);
                insertedPeopleBuffered[2].SomeName.ShouldBe("P3");
                insertedPeopleBuffered[2].Age.ShouldBe(30);

                insertedPeopleBuffered[3].SomeId.ShouldBe(4);
                insertedPeopleBuffered[3].SomeName.ShouldBe("P4");
                insertedPeopleBuffered[3].Age.ShouldBe(40);

                insertedPeopleUnBuffered[0].SomeId.ShouldBe(1);
                insertedPeopleUnBuffered[0].SomeName.ShouldBe("P1");
                insertedPeopleUnBuffered[0].Age.ShouldBe(10);

                insertedPeopleUnBuffered[1].SomeId.ShouldBe(2);
                insertedPeopleUnBuffered[1].SomeName.ShouldBe("P2");
                insertedPeopleUnBuffered[1].Age.ShouldBe(20);

                insertedPeopleUnBuffered[2].SomeId.ShouldBe(3);
                insertedPeopleUnBuffered[2].SomeName.ShouldBe("P3");
                insertedPeopleUnBuffered[2].Age.ShouldBe(30);

                insertedPeopleUnBuffered[3].SomeId.ShouldBe(4);
                insertedPeopleUnBuffered[3].SomeName.ShouldBe("P4");
                insertedPeopleUnBuffered[3].Age.ShouldBe(40);
            }
        }

        private static async Task When_getting_non_aliased_models()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 40 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                var insertedPeopleBuffered = (await repo.Get()).ToArray();
                var insertedPeopleUnBuffered = (await repo.Get()).ToArray();

                insertedPeopleBuffered.Length.ShouldBe(people.Length);
                insertedPeopleUnBuffered.Length.ShouldBe(people.Length);

                insertedPeopleBuffered[0].Id.ShouldBe(1);
                insertedPeopleBuffered[0].Name.ShouldBe("P1");
                insertedPeopleBuffered[0].Age.ShouldBe(10);

                insertedPeopleBuffered[1].Id.ShouldBe(2);
                insertedPeopleBuffered[1].Name.ShouldBe("P2");
                insertedPeopleBuffered[1].Age.ShouldBe(20);

                insertedPeopleBuffered[2].Id.ShouldBe(3);
                insertedPeopleBuffered[2].Name.ShouldBe("P3");
                insertedPeopleBuffered[2].Age.ShouldBe(30);

                insertedPeopleBuffered[3].Id.ShouldBe(4);
                insertedPeopleBuffered[3].Name.ShouldBe("P4");
                insertedPeopleBuffered[3].Age.ShouldBe(40);

                insertedPeopleUnBuffered[0].Id.ShouldBe(1);
                insertedPeopleUnBuffered[0].Name.ShouldBe("P1");
                insertedPeopleUnBuffered[0].Age.ShouldBe(10);

                insertedPeopleUnBuffered[1].Id.ShouldBe(2);
                insertedPeopleUnBuffered[1].Name.ShouldBe("P2");
                insertedPeopleUnBuffered[1].Age.ShouldBe(20);

                insertedPeopleUnBuffered[2].Id.ShouldBe(3);
                insertedPeopleUnBuffered[2].Name.ShouldBe("P3");
                insertedPeopleUnBuffered[2].Age.ShouldBe(30);

                insertedPeopleUnBuffered[3].Id.ShouldBe(4);
                insertedPeopleUnBuffered[3].Name.ShouldBe("P4");
                insertedPeopleUnBuffered[3].Age.ShouldBe(40);
            }
        }

        private static async Task When_getting_aliased_models()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 40 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                var insertedPeopleBuffered = (await repo.Get()).ToArray();
                var insertedPeopleUnBuffered = (await repo.Get()).ToArray();

                insertedPeopleBuffered.Length.ShouldBe(people.Length);
                insertedPeopleUnBuffered.Length.ShouldBe(people.Length);

                insertedPeopleBuffered[0].SomeId.ShouldBe(1);
                insertedPeopleBuffered[0].SomeName.ShouldBe("P1");
                insertedPeopleBuffered[0].Age.ShouldBe(10);

                insertedPeopleBuffered[1].SomeId.ShouldBe(2);
                insertedPeopleBuffered[1].SomeName.ShouldBe("P2");
                insertedPeopleBuffered[1].Age.ShouldBe(20);

                insertedPeopleBuffered[2].SomeId.ShouldBe(3);
                insertedPeopleBuffered[2].SomeName.ShouldBe("P3");
                insertedPeopleBuffered[2].Age.ShouldBe(30);

                insertedPeopleBuffered[3].SomeId.ShouldBe(4);
                insertedPeopleBuffered[3].SomeName.ShouldBe("P4");
                insertedPeopleBuffered[3].Age.ShouldBe(40);

                insertedPeopleUnBuffered[0].SomeId.ShouldBe(1);
                insertedPeopleUnBuffered[0].SomeName.ShouldBe("P1");
                insertedPeopleUnBuffered[0].Age.ShouldBe(10);

                insertedPeopleUnBuffered[1].SomeId.ShouldBe(2);
                insertedPeopleUnBuffered[1].SomeName.ShouldBe("P2");
                insertedPeopleUnBuffered[1].Age.ShouldBe(20);

                insertedPeopleUnBuffered[2].SomeId.ShouldBe(3);
                insertedPeopleUnBuffered[2].SomeName.ShouldBe("P3");
                insertedPeopleUnBuffered[2].Age.ShouldBe(30);

                insertedPeopleUnBuffered[3].SomeId.ShouldBe(4);
                insertedPeopleUnBuffered[3].SomeName.ShouldBe("P4");
                insertedPeopleUnBuffered[3].Age.ShouldBe(40);
            }
        }

        private static async Task When_getting_non_aliased_models_by_selector()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 10 },
                    new Person { Name = "P5", Age = 40 },
                    new Person { Name = "P5", Age = 10 }
                };

                (await repo.Insert(people)).ShouldBe(6);

                var onePerson = (await repo.GetWhere(p => p.Id, 1)).ToArray();

                onePerson.Length.ShouldBe(1);
                onePerson[0].Id.ShouldBe(1);
                onePerson[0].Name.ShouldBe("P1");
                onePerson[0].Age.ShouldBe(10);

                var peopleWithSameName = (await repo.GetWhere(p => p.Name, "P5")).ToArray();

                peopleWithSameName.Length.ShouldBe(2);
                peopleWithSameName[0].Id.ShouldBe(5);
                peopleWithSameName[0].Name.ShouldBe("P5");
                peopleWithSameName[0].Age.ShouldBe(40);

                peopleWithSameName[1].Id.ShouldBe(6);
                peopleWithSameName[1].Name.ShouldBe("P5");
                peopleWithSameName[1].Age.ShouldBe(10);

                var peopleWithSameAge = (await repo.GetWhere(p => p.Age, 10)).ToArray();

                peopleWithSameAge.Length.ShouldBe(3);
                peopleWithSameAge[0].Id.ShouldBe(1);
                peopleWithSameAge[0].Name.ShouldBe("P1");
                peopleWithSameAge[0].Age.ShouldBe(10);

                peopleWithSameAge[1].Id.ShouldBe(4);
                peopleWithSameAge[1].Name.ShouldBe("P4");
                peopleWithSameAge[1].Age.ShouldBe(10);

                peopleWithSameAge[2].Id.ShouldBe(6);
                peopleWithSameAge[2].Name.ShouldBe("P5");
                peopleWithSameAge[2].Age.ShouldBe(10);

                var peopleWithDifferentNames = (await repo.GetWhere(p => p.Name, null, "P1", "P2")).ToArray();

                peopleWithDifferentNames.Length.ShouldBe(2);

                peopleWithDifferentNames[0].Id.ShouldBe(1);
                peopleWithDifferentNames[0].Name.ShouldBe("P1");
                peopleWithDifferentNames[0].Age.ShouldBe(10);

                peopleWithDifferentNames[1].Id.ShouldBe(2);
                peopleWithDifferentNames[1].Name.ShouldBe("P2");
                peopleWithDifferentNames[1].Age.ShouldBe(20);

                var nonExistingPeople = (await repo.GetWhere(p => p.Age, 60)).ToArray();

                nonExistingPeople.ShouldBeEmpty();
            }
        }

        private static async Task When_getting_aliased_models_by_selector()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 10 },
                    new MyPerson { SomeName = "P5", Age = 40 },
                    new MyPerson { SomeName = "P5", Age = 10 }
                };

                (await repo.Insert(people)).ShouldBe(6);

                var onePerson = (await repo.GetWhere(p => p.SomeId, 1)).ToArray();

                onePerson.Length.ShouldBe(1);
                onePerson[0].SomeId.ShouldBe(1);
                onePerson[0].SomeName.ShouldBe("P1");
                onePerson[0].Age.ShouldBe(10);

                var peopleWithSameName = (await repo.GetWhere(p => p.SomeName, "P5")).ToArray();

                peopleWithSameName.Length.ShouldBe(2);
                peopleWithSameName[0].SomeId.ShouldBe(5);
                peopleWithSameName[0].SomeName.ShouldBe("P5");
                peopleWithSameName[0].Age.ShouldBe(40);

                peopleWithSameName[1].SomeId.ShouldBe(6);
                peopleWithSameName[1].SomeName.ShouldBe("P5");
                peopleWithSameName[1].Age.ShouldBe(10);

                var peopleWithSameAge = (await repo.GetWhere(p => p.Age, 10)).ToArray();

                peopleWithSameAge.Length.ShouldBe(3);
                peopleWithSameAge[0].SomeId.ShouldBe(1);
                peopleWithSameAge[0].SomeName.ShouldBe("P1");
                peopleWithSameAge[0].Age.ShouldBe(10);

                peopleWithSameAge[1].SomeId.ShouldBe(4);
                peopleWithSameAge[1].SomeName.ShouldBe("P4");
                peopleWithSameAge[1].Age.ShouldBe(10);

                peopleWithSameAge[2].SomeId.ShouldBe(6);
                peopleWithSameAge[2].SomeName.ShouldBe("P5");
                peopleWithSameAge[2].Age.ShouldBe(10);

                var peopleWithDifferentNames = (await repo.GetWhere(p => p.SomeName, null, "P1", "P2")).ToArray();

                peopleWithDifferentNames.Length.ShouldBe(2);

                peopleWithDifferentNames[0].SomeId.ShouldBe(1);
                peopleWithDifferentNames[0].SomeName.ShouldBe("P1");
                peopleWithDifferentNames[0].Age.ShouldBe(10);

                peopleWithDifferentNames[1].SomeId.ShouldBe(2);
                peopleWithDifferentNames[1].SomeName.ShouldBe("P2");
                peopleWithDifferentNames[1].Age.ShouldBe(20);

                var nonExistingPeople = (await repo.GetWhere(p => p.Age, 60)).ToArray();

                nonExistingPeople.ShouldBeEmpty();
            }
        }

        private static async Task When_getting_non_aliased_models_by_filter()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var ctx = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await ctx.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await ctx.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 10 },
                    new Person { Name = "P5", Age = 40 },
                    new Person { Name = "P5", Age = 50 }
                };

                (await ctx.Insert(people)).ShouldBe(6);

                var filter1 = ctx.Query.Filter
                    .And(p => p.Age, Operator.GreaterThan, 30)
                    .Or(p => p.Name, Operator.Equal, "P1");

                var filter1Result = (await ctx.GetWhere(filter1)).ToArray();
                filter1Result.Length.ShouldBe(3);

                filter1Result[0].Id.ShouldBe(1);
                filter1Result[0].Name.ShouldBe("P1");
                filter1Result[0].Age.ShouldBe(10);

                filter1Result[1].Id.ShouldBe(5);
                filter1Result[1].Name.ShouldBe("P5");
                filter1Result[1].Age.ShouldBe(40);

                filter1Result[2].Id.ShouldBe(6);
                filter1Result[2].Name.ShouldBe("P5");
                filter1Result[2].Age.ShouldBe(50);

                var filter2 = ctx.Query.Filter
                    .And(p => p.Age, Operator.GreaterThanOrEqual, 50)
                    .Or(p => p.Age, Operator.Equal, 20);

                var filter2Result = (await ctx.GetWhere(filter2)).ToArray();
                filter2Result.Length.ShouldBe(2);

                filter2Result[0].Id.ShouldBe(2);
                filter2Result[0].Name.ShouldBe("P2");
                filter2Result[0].Age.ShouldBe(20);

                filter2Result[1].Id.ShouldBe(6);
                filter2Result[1].Name.ShouldBe("P5");
                filter2Result[1].Age.ShouldBe(50);

                var filter3 = ctx.Query.Filter
                    .And(p => p.Age, Operator.Equal, 50)
                    .OrIn(p => p.Name, new[] { "P2", "P3", "P4" });

                var filter3Result = (await ctx.GetWhere(filter3))
                    .OrderBy(x => x.Name)
                    .ToArray();
                filter3Result.Length.ShouldBe(4);

                filter3Result[0].Id.ShouldBe(2);
                filter3Result[0].Name.ShouldBe("P2");
                filter3Result[0].Age.ShouldBe(20);

                filter3Result[1].Id.ShouldBe(3);
                filter3Result[1].Name.ShouldBe("P3");
                filter3Result[1].Age.ShouldBe(30);

                filter3Result[2].Id.ShouldBe(4);
                filter3Result[2].Name.ShouldBe("P4");
                filter3Result[2].Age.ShouldBe(10);

                filter3Result[3].Id.ShouldBe(6);
                filter3Result[3].Name.ShouldBe("P5");
                filter3Result[3].Age.ShouldBe(50);
            }
        }

        private static async Task When_getting_aliased_models_by_filter()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var ctx = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await ctx.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await ctx.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 10 },
                    new MyPerson { SomeName = "P5", Age = 40 },
                    new MyPerson { SomeName = "P5", Age = 50 }
                };

                (await ctx.Insert(people)).ShouldBe(6);

                var filter1 = ctx.Query.Filter
                    .And(p => p.Age, Operator.GreaterThan, 30)
                    .Or(p => p.SomeName, Operator.Equal, "P1");

                var filter1Result = (await ctx.GetWhere(filter1)).ToArray();
                filter1Result.Length.ShouldBe(3);

                filter1Result[0].SomeId.ShouldBe(1);
                filter1Result[0].SomeName.ShouldBe("P1");
                filter1Result[0].Age.ShouldBe(10);

                filter1Result[1].SomeId.ShouldBe(5);
                filter1Result[1].SomeName.ShouldBe("P5");
                filter1Result[1].Age.ShouldBe(40);

                filter1Result[2].SomeId.ShouldBe(6);
                filter1Result[2].SomeName.ShouldBe("P5");
                filter1Result[2].Age.ShouldBe(50);

                var filter2 = ctx.Query.Filter
                    .And(p => p.Age, Operator.GreaterThanOrEqual, 50)
                    .Or(p => p.Age, Operator.Equal, 20);

                var filter2Result = (await ctx.GetWhere(filter2)).ToArray();
                filter2Result.Length.ShouldBe(2);

                filter2Result[0].SomeId.ShouldBe(2);
                filter2Result[0].SomeName.ShouldBe("P2");
                filter2Result[0].Age.ShouldBe(20);

                filter2Result[1].SomeId.ShouldBe(6);
                filter2Result[1].SomeName.ShouldBe("P5");
                filter2Result[1].Age.ShouldBe(50);

                var filter3 = ctx.Query.Filter
                    .And(p => p.Age, Operator.Equal, 50)
                    .OrIn(p => p.SomeName, new[] { "P2", "P3", "P4" });

                var filter3Result = (await ctx.GetWhere(filter3))
                    .OrderBy(x => x.SomeName)
                    .ToArray();
                filter3Result.Length.ShouldBe(4);

                filter3Result[0].SomeId.ShouldBe(2);
                filter3Result[0].SomeName.ShouldBe("P2");
                filter3Result[0].Age.ShouldBe(20);

                filter3Result[1].SomeId.ShouldBe(3);
                filter3Result[1].SomeName.ShouldBe("P3");
                filter3Result[1].Age.ShouldBe(30);

                filter3Result[2].SomeId.ShouldBe(4);
                filter3Result[2].SomeName.ShouldBe("P4");
                filter3Result[2].Age.ShouldBe(10);

                filter3Result[3].SomeId.ShouldBe(6);
                filter3Result[3].SomeName.ShouldBe("P5");
                filter3Result[3].Age.ShouldBe(50);
            }
        }

        private static async Task When_inserting_single_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var p1 = new Person { Name = "P1", Age = 10 };
                ((long)await repo.Insert(p1)).ShouldBe(1);

                (await repo.Get()).Count.ShouldBe(1);

                var insertedP1 = (await repo.Get()).Single();
                insertedP1.ShouldNotBeNull();
                insertedP1.Id.ShouldBe(1);
                insertedP1.Name.ShouldBe("P1");
                insertedP1.Age.ShouldBe(10);

                var p2 = new Person { Name = "P2", Age = 20 };
                ((long)await repo.Insert(p2)).ShouldBe(2);

                (await repo.Get()).Count.ShouldBe(2);

                var insertedP2 = (await repo.GetWhere(p => p.Id, 2)).Single();
                insertedP2.ShouldNotBeNull();
                insertedP2.Id.ShouldBe(2);
                insertedP2.Name.ShouldBe("P2");
                insertedP2.Age.ShouldBe(20);

                var p3 = new Person { Id = 1, Name = "P3", Age = 30 };
                ((long)await repo.Insert(p3)).ShouldBe(3);

                (await repo.Get()).Count.ShouldBe(3);

                var insertedP3 = (await repo.GetWhere(p => p.Id, 3)).Single();
                insertedP3.ShouldNotBeNull();
                insertedP3.Id.ShouldBe(3);
                insertedP3.Name.ShouldBe("P3");
                insertedP3.Age.ShouldBe(30);

                var p4 = new Person { Id = 4, Name = "P4", Age = 40 };
                ((long)await repo.Insert(p4)).ShouldBe(4);

                (await repo.Get()).Count.ShouldBe(4);

                var insertedP4 = (await repo.GetWhere(p => p.Id, 4)).Single();
                insertedP4.ShouldNotBeNull();
                insertedP4.Id.ShouldBe(4);
                insertedP4.Name.ShouldBe("P4");
                insertedP4.Age.ShouldBe(40);
            }
        }

        private static async Task When_inserting_single_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var p1 = new MyPerson { SomeName = "P1", Age = 10 };
                ((long)await repo.Insert(p1)).ShouldBe(1);

                (await repo.Get()).Count.ShouldBe(1);

                var insertedP1 = (await repo.Get()).Single();
                insertedP1.ShouldNotBeNull();
                insertedP1.SomeId.ShouldBe(1);
                insertedP1.SomeName.ShouldBe("P1");
                insertedP1.Age.ShouldBe(10);

                var p2 = new MyPerson { SomeName = "P2", Age = 20 };
                ((long)await repo.Insert(p2)).ShouldBe(2);

                (await repo.Get()).Count.ShouldBe(2);

                var insertedP2 = (await repo.GetWhere(p => p.SomeId, 2)).Single();
                insertedP2.ShouldNotBeNull();
                insertedP2.SomeId.ShouldBe(2);
                insertedP2.SomeName.ShouldBe("P2");
                insertedP2.Age.ShouldBe(20);

                var p3 = new MyPerson { SomeId = 1, SomeName = "P3", Age = 30 };
                ((long)await repo.Insert(p3)).ShouldBe(3);

                (await repo.Get()).Count.ShouldBe(3);

                var insertedP3 = (await repo.GetWhere(p => p.SomeId, 3)).Single();
                insertedP3.ShouldNotBeNull();
                insertedP3.SomeId.ShouldBe(3);
                insertedP3.SomeName.ShouldBe("P3");
                insertedP3.Age.ShouldBe(30);

                var p4 = new MyPerson { SomeId = 4, SomeName = "P4", Age = 40 };
                ((long)await repo.Insert(p4)).ShouldBe(4);

                (await repo.Get()).Count.ShouldBe(4);

                var insertedP4 = (await repo.GetWhere(p => p.SomeId, 4)).Single();
                insertedP4.ShouldNotBeNull();
                insertedP4.SomeId.ShouldBe(4);
                insertedP4.SomeName.ShouldBe("P4");
                insertedP4.Age.ShouldBe(40);
            }
        }

        private static async Task When_inserting_multiple_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 40 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                var insertedPeople = (await repo.Get()).ToArray();

                insertedPeople.Length.ShouldBe(4);

                var p1 = insertedPeople[0];
                p1.Id.ShouldBe(1);
                p1.Name.ShouldBe("P1");
                p1.Age.ShouldBe(10);

                var p2 = insertedPeople[1];
                p2.Id.ShouldBe(2);
                p2.Name.ShouldBe("P2");
                p2.Age.ShouldBe(20);

                var p3 = insertedPeople[2];
                p3.Id.ShouldBe(3);
                p3.Name.ShouldBe("P3");
                p3.Age.ShouldBe(30);

                var p4 = insertedPeople[3];
                p4.Id.ShouldBe(4);
                p4.Name.ShouldBe("P4");
                p4.Age.ShouldBe(40);

                var emptyCollection = Enumerable.Empty<Person>();
                (await repo.Insert(emptyCollection)).ShouldBe(0);
            }
        }

        private static async Task When_inserting_multiple_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 40 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                var insertedPeople = (await repo.Get()).ToArray();

                insertedPeople.Length.ShouldBe(4);

                var p1 = insertedPeople[0];
                p1.SomeId.ShouldBe(1);
                p1.SomeName.ShouldBe("P1");
                p1.Age.ShouldBe(10);

                var p2 = insertedPeople[1];
                p2.SomeId.ShouldBe(2);
                p2.SomeName.ShouldBe("P2");
                p2.Age.ShouldBe(20);

                var p3 = insertedPeople[2];
                p3.SomeId.ShouldBe(3);
                p3.SomeName.ShouldBe("P3");
                p3.Age.ShouldBe(30);

                var p4 = insertedPeople[3];
                p4.SomeId.ShouldBe(4);
                p4.SomeName.ShouldBe("P4");
                p4.Age.ShouldBe(40);

                var emptyCollection = Enumerable.Empty<MyPerson>();
                (await repo.Insert(emptyCollection)).ShouldBe(0);
            }
        }

        private static async Task When_inserting_model_with_no_identity_column()
        {
            // ReSharper disable once InconsistentNaming
            const string tableQuery = "IF OBJECT_ID('PersonTemp', 'U') IS NULL"
                                              + " CREATE TABLE PersonTemp ("
                                              + " [Key] INTEGER NOT NULL,"
                                              + " Name NVARCHAR(50) NULL,"
                                              + " Age INTEGER NOT NULL);";

            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<PersonTempNoIdentity>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(tableQuery);
                await repo.DeleteAll();

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new PersonTempNoIdentity { Id = 1, Name = "P1", Age = 10 },
                    new PersonTempNoIdentity { Id = 2, Name = "P2", Age = 20 },
                    new PersonTempNoIdentity { Id = 3, Name = "P3", Age = 30 },
                    new PersonTempNoIdentity { Id = 7, Name = "P4", Age = 40 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                var insertedPeople = (await repo.Get()).ToArray();

                insertedPeople.Length.ShouldBe(4);

                var p1 = insertedPeople[0];
                p1.Id.ShouldBe(1);
                p1.Name.ShouldBe("P1");
                p1.Age.ShouldBe(10);

                var p2 = insertedPeople[1];
                p2.Id.ShouldBe(2);
                p2.Name.ShouldBe("P2");
                p2.Age.ShouldBe(20);

                var p3 = insertedPeople[2];
                p3.Id.ShouldBe(3);
                p3.Name.ShouldBe("P3");
                p3.Age.ShouldBe(30);

                var p4 = insertedPeople[3];
                p4.Id.ShouldBe(7);
                p4.Name.ShouldBe("P4");
                p4.Age.ShouldBe(40);
            }
        }

        private static async Task When_inserting_model_with_string_identity_column()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<TableWithStringIdQuery>(SQLServerDialect.Instance);
                await conn.ExecuteAsync(TableWithStringIdQuery);
                await repo.DeleteAll();

                (await repo.Count(p => p.Id)).ShouldBe((ulong)0);

                var single = new TableWithStringIdQuery { Id = "Foo", Name = "Bar", Age = 1 };

                var multiple = new[]
                {
                    new TableWithStringIdQuery { Id = "John", Name = "Doe", Age = 2},
                    new TableWithStringIdQuery { Id = "John", Name = "Foe", Age = 3},
                    new TableWithStringIdQuery { Id = "Bob", Name = "Doe", Age = 4}
                };

                string id = await repo.Insert(single);
                id.ShouldBe("Foo");

                var inserted = await repo.Insert(multiple);
                inserted.ShouldBe(3);

                (await repo.Count(p => p.Id)).ShouldBe((ulong)4);

                var all = (await repo.Get()).OrderBy(x => x.Age).ToArray();
                all[0].Id.ShouldBe("Foo");
                all[0].Name.ShouldBe("Bar");
                all[0].Age.ShouldBe(1);

                all[1].Id.ShouldBe("John");
                all[1].Name.ShouldBe("Doe");
                all[1].Age.ShouldBe(2);

                all[2].Id.ShouldBe("John");
                all[2].Name.ShouldBe("Foe");
                all[2].Age.ShouldBe(3);

                all[3].Id.ShouldBe("Bob");
                all[3].Name.ShouldBe("Doe");
                all[3].Age.ShouldBe(4);
            }
        }

        private static async Task When_inserting_model_with_guid_identity_column()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<TableWithGuidIdQuery>(SQLServerDialect.Instance);
                await conn.ExecuteAsync(TableWithGuidIdQuery);
                await repo.DeleteAll();

                (await repo.Count(p => p.Id)).ShouldBe((ulong)0);

                var guid1 = Guid.NewGuid();
                var single = new TableWithGuidIdQuery { Id = guid1, Name = "Bar", Age = 1 };

                var guid2 = Guid.NewGuid();
                var guid3 = Guid.NewGuid();
                var guid4 = Guid.NewGuid();

                var multiple = new[]
                {
                    new TableWithGuidIdQuery { Id = guid2, Name = "Doe", Age = 2},
                    new TableWithGuidIdQuery { Id = guid3, Name = "Foe", Age = 3},
                    new TableWithGuidIdQuery { Id = guid4, Name = "Doe", Age = 4}
                };

                Guid id = await repo.Insert(single);
                id.ShouldBe(guid1);

                var inserted = await repo.Insert(multiple);
                inserted.ShouldBe(3);

                (await repo.Count(p => p.Id)).ShouldBe((ulong)4);

                var all = (await repo.Get()).OrderBy(x => x.Age).ToArray();
                all[0].Id.ShouldBe(guid1);
                all[0].Name.ShouldBe("Bar");
                all[0].Age.ShouldBe(1);

                all[1].Id.ShouldBe(guid2);
                all[1].Name.ShouldBe("Doe");
                all[1].Age.ShouldBe(2);

                all[2].Id.ShouldBe(guid3);
                all[2].Name.ShouldBe("Foe");
                all[2].Age.ShouldBe(3);

                all[3].Id.ShouldBe(guid4);
                all[3].Name.ShouldBe("Doe");
                all[3].Age.ShouldBe(4);
            }
        }

        private static async Task When_inserting_partial_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var newItemWithIdentity = new
                {
                    Id = 1,
                    Age = 10,
                    Name = "P1"
                };

                Should.Throw<SqlException>(async () => await repo.InsertPartial(newItemWithIdentity));

                var newItemWithNoIdentity = new
                {
                    Age = 20,
                    Name = "P2"
                };

                var insertedIdWithNoIdentity = (long)await repo.InsertPartial(newItemWithNoIdentity);
                insertedIdWithNoIdentity.ShouldBe(1);

                var person = new Person { Name = "P3" };

                var insertedPerson = (long) await repo.Insert(person);
                insertedPerson.ShouldBe(2);

                var allItems = (await repo.Get()).OrderBy(x => x.Id).ToArray();
                allItems.Length.ShouldBe(2);

                allItems[0].Id.ShouldBe(1);
                allItems[0].Age.ShouldBe(20);
                allItems[0].Name.ShouldBe("P2");

                allItems[1].Id.ShouldBe(2);
                allItems[1].Age.ShouldBe(0);
                allItems[1].Name.ShouldBe("P3");
            }
        }

        private static async Task When_inserting_partial_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var newItemWithIdentity = new
                {
                    SomeId = 1,
                    Age = 10,
                    SomeName = "P1"
                };

                Should.Throw<SqlException>(async () => await repo.InsertPartial(newItemWithIdentity));

                var newItemWithNoIdentity = new
                {
                    Age = 20,
                    SomeName = "P2"
                };

                var insertedIdWithNoIdentity = (long)await repo.InsertPartial(newItemWithNoIdentity);
                insertedIdWithNoIdentity.ShouldBe(1);

                var person = new MyPerson { SomeName = "P3" };

                var insertedPerson = (long)await repo.Insert(person);
                insertedPerson.ShouldBe(2);

                var allItems = (await repo.Get()).OrderBy(x => x.SomeId).ToArray();
                allItems.Length.ShouldBe(2);

                allItems[0].SomeId.ShouldBe(1);
                allItems[0].Age.ShouldBe(20);
                allItems[0].SomeName.ShouldBe("P2");

                allItems[1].SomeId.ShouldBe(2);
                allItems[1].Age.ShouldBe(0);
                allItems[1].SomeName.ShouldBe("P3");
            }
        }

        private static async Task When_inserting_multiple_partial_non_aliased_models()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var batch1 = new[]
                {
                    new
                    {
                        Age = 10,
                        Name = "P1"
                    },
                    new
                    {
                        Age = 20,
                        Name = "P2"
                    }
                };

                var batch2 = new[]
                {
                    new Person
                    {
                        Age = 30,
                        Name = "P3"
                    },
                    new Person
                    {
                        Age = 40,
                        Name = "P4"
                    }
                };

                var batch1InsertedCount = await repo.InsertPartial(batch1);
                batch1InsertedCount.ShouldBe(2);

                Should.Throw<SqlException>(async () => await repo.InsertPartial(batch2))
                    .Message.ShouldBe("Cannot insert explicit value for identity column in table 'Person' when IDENTITY_INSERT is set to OFF.");

                var allItems = (await repo.Get()).OrderBy(x => x.Id).ToArray();
                allItems.Length.ShouldBe(2);

                allItems[0].Id.ShouldBe(1);
                allItems[0].Age.ShouldBe(10);
                allItems[0].Name.ShouldBe("P1");

                allItems[1].Id.ShouldBe(2);
                allItems[1].Age.ShouldBe(20);
                allItems[1].Name.ShouldBe("P2");

                var emptyCollection = Enumerable.Empty<object>();
                (await repo.InsertPartial(emptyCollection)).ShouldBe(0);
            }
        }

        private static async Task When_inserting_multiple_partial_aliased_models()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var batch1 = new[]
                {
                    new
                    {
                        Age = 10,
                        SomeName = "P1"
                    },
                    new
                    {
                        Age = 20,
                        SomeName = "P2"
                    }
                };

                var batch2 = new[]
                {
                    new MyPerson
                    {
                        Age = 30,
                        SomeName = "P3"
                    },
                    new MyPerson
                    {
                        Age = 40,
                        SomeName = "P4"
                    }
                };

                var batch1InsertedCount = await repo.InsertPartial(batch1);
                batch1InsertedCount.ShouldBe(2);

                Should.Throw<SqlException>(async () => await repo.InsertPartial(batch2))
                    .Message.ShouldBe("Cannot insert explicit value for identity column in table 'Person' when IDENTITY_INSERT is set to OFF.");

                var allItems = (await repo.Get()).OrderBy(x => x.SomeId).ToArray();
                allItems.Length.ShouldBe(2);

                allItems[0].SomeId.ShouldBe(1);
                allItems[0].Age.ShouldBe(10);
                allItems[0].SomeName.ShouldBe("P1");

                allItems[1].SomeId.ShouldBe(2);
                allItems[1].Age.ShouldBe(20);
                allItems[1].SomeName.ShouldBe("P2");

                var emptyCollection = Enumerable.Empty<object>();
                (await repo.InsertPartial(emptyCollection)).ShouldBe(0);
            }
        }

        private static async Task When_updating_non_aliased_model()
        {
            /*
             * Updating Person with nothing else included should update
             * all columns for the item EXCEPT THE ID of that record
             */
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Count(p => p.Id)).ShouldBe((ulong)0);

                var peopleToInsert = new[]
                {
                    new Person{ Age = 10, Name = "P1"},
                    new Person{ Age = 20, Name = "P2"},
                    new Person{ Age = 30, Name = "P3"}
                };

                (await repo.Insert(peopleToInsert)).ShouldBe(3);

                var peopleInserted = (await repo.Get()).ToArray();
                peopleInserted.Length.ShouldBe(3);

                peopleInserted[0].Id.ShouldBe(1);

                var personToUpdate = new Person
                {
                    Id = 0,
                    Age = 100
                };

                (await repo.Update(personToUpdate)).ShouldBe(0);

                personToUpdate.Id = peopleInserted[0].Id;
                Should.Throw<SqlException>(async () => await repo.Update(personToUpdate))
                    .Message.ShouldBe("Cannot insert the value NULL into column 'Name', table 'SandBox.dbo.Person'; column does not allow nulls. UPDATE fails.\r\nThe statement has been terminated.");

                personToUpdate.Name = peopleInserted[0].Name;
                (await repo.Update(personToUpdate)).ShouldBe(1);
                (await repo.GetWhere(p => p.Id, personToUpdate.Id)).Single().Id.ShouldBe(1);
                (await repo.GetWhere(p => p.Id, personToUpdate.Id)).Single().Age.ShouldBe(100);
                (await repo.GetWhere(p => p.Id, personToUpdate.Id)).Single().Name.ShouldBe("P1");

                var anotherPersonToUpdate = new Person
                {
                    Id = 3,
                    Age = 66,
                    Name = "P3 - Updated"
                };

                (await repo.Update(anotherPersonToUpdate)).ShouldBe(1);
                (await repo.GetWhere(p => p.Id, anotherPersonToUpdate.Id)).Single().Id.ShouldBe(3);
                (await repo.GetWhere(p => p.Id, anotherPersonToUpdate.Id)).Single().Age.ShouldBe(66);
                (await repo.GetWhere(p => p.Id, anotherPersonToUpdate.Id)).Single().Name.ShouldBe("P3 - Updated");

                (await repo.Count(p => p.Id)).ShouldBe((ulong)3);
            }
        }

        private static async Task When_updating_aliased_model()
        {
            /*
             * Updating Person with nothing else included should update
             * all columns for the item EXCEPT THE ID of that record
             */
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Count(p => p.SomeId)).ShouldBe((ulong)0);

                var peopleToInsert = new[]
                {
                    new MyPerson{ Age = 10, SomeName = "P1"},
                    new MyPerson{ Age = 20, SomeName = "P2"},
                    new MyPerson{ Age = 30, SomeName = "P3"}
                };

                (await repo.Insert(peopleToInsert)).ShouldBe(3);

                var peopleInserted = (await repo.Get()).ToArray();
                peopleInserted.Length.ShouldBe(3);

                peopleInserted[0].SomeId.ShouldBe(1);

                var personToUpdate = new MyPerson
                {
                    SomeId = 0,
                    Age = 100
                };

                (await repo.Update(personToUpdate)).ShouldBe(0);

                personToUpdate.SomeId = peopleInserted[0].SomeId;
                Should.Throw<SqlException>(async () => await repo.Update(personToUpdate))
                    .Message.ShouldBe("Cannot insert the value NULL into column 'Name', table 'SandBox.dbo.Person'; column does not allow nulls. UPDATE fails.\r\nThe statement has been terminated.");

                personToUpdate.SomeName = peopleInserted[0].SomeName;
                (await repo.Update(personToUpdate)).ShouldBe(1);
                (await repo.GetWhere(p => p.SomeId, personToUpdate.SomeId)).Single().SomeId.ShouldBe(1);
                (await repo.GetWhere(p => p.SomeId, personToUpdate.SomeId)).Single().Age.ShouldBe(100);
                (await repo.GetWhere(p => p.SomeId, personToUpdate.SomeId)).Single().SomeName.ShouldBe("P1");

                var anotherPersonToUpdate = new MyPerson
                {
                    SomeId = 3,
                    Age = 66,
                    SomeName = "P3 - Updated"
                };

                (await repo.Update(anotherPersonToUpdate)).ShouldBe(1);
                (await repo.GetWhere(p => p.SomeId, anotherPersonToUpdate.SomeId)).Single().SomeId.ShouldBe(3);
                (await repo.GetWhere(p => p.SomeId, anotherPersonToUpdate.SomeId)).Single().Age.ShouldBe(66);
                (await repo.GetWhere(p => p.SomeId, anotherPersonToUpdate.SomeId)).Single().SomeName.ShouldBe("P3 - Updated");

                (await repo.Count(p => p.SomeId)).ShouldBe((ulong)3);
            }
        }

        private static async Task When_updating_non_aliased_model_with_filter()
        {
            /*
             * Updating Person with nothing else included should update
             * all columns for the item EXCEPT THE ID of that record
             */
            using (var conn = new SqlConnection(ConnectionString))
            {
                var ctx = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await ctx.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await ctx.Count(p => p.Id)).ShouldBe((ulong)0);

                var peopleToInsert = new[]
                {
                    new Person{ Age = 10, Name = "P1"},
                    new Person{ Age = 20, Name = "P2"},
                    new Person{ Age = 30, Name = "P3"}
                };

                (await ctx.Insert(peopleToInsert)).ShouldBe(3);

                var peopleInserted = (await ctx.Get()).ToArray();
                peopleInserted.Length.ShouldBe(3);

                peopleInserted[0].Id.ShouldBe(1);

                var personToUpdate = new Person
                {
                    Id = 0,
                    Age = 100
                };

                var filter = ctx.Query.Filter.And(p => p.Id, Operator.Equal, 0);
                Should.Throw<SqlException>(async () => await ctx.UpdateWhere(personToUpdate, filter))
                    .Message.ShouldBe("Cannot update identity column 'Id'.");

                filter = ctx.Query.Filter.And(p => p.Id, Operator.Equal, peopleInserted[0].Id);

                Should.Throw<SqlException>(async () => await ctx.UpdateWhere(personToUpdate, filter))
                    .Message.ShouldBe("Cannot update identity column 'Id'.");

                personToUpdate.Name = peopleInserted[0].Name;

                Should.Throw<SqlException>(async () => await ctx.UpdateWhere(personToUpdate, filter))
                    .Message.ShouldBe("Cannot update identity column 'Id'.");

                var anotherPersonToUpdate = new Person
                {
                    Id = 666,
                    Age = 66,
                    Name = "P3 - Updated"
                };

                filter = ctx.Query.Filter.And(p => p.Id, Operator.Equal, 3);

                Should.Throw<SqlException>(async () => await ctx.UpdateWhere(anotherPersonToUpdate, filter))
                    .Message.ShouldBe("Cannot update identity column 'Id'.");

                (await ctx.Count(p => p.Id)).ShouldBe((ulong)3);
                (await ctx.Count(p => p.Id, ctx.Query.Filter.And(p => p.Id, Operator.Equal, 3))).ShouldBe((ulong)1);
            }
        }

        private static async Task When_updating_aliased_model_with_filter()
        {
            /*
             * Updating Person with nothing else included should update
             * all columns for the item EXCEPT THE ID of that record
             */
            using (var conn = new SqlConnection(ConnectionString))
            {
                var ctx = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await ctx.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await ctx.Count(p => p.SomeId)).ShouldBe((ulong)0);

                var peopleToInsert = new[]
                {
                    new MyPerson{ Age = 10, SomeName = "P1"},
                    new MyPerson{ Age = 20, SomeName = "P2"},
                    new MyPerson{ Age = 30, SomeName = "P3"}
                };

                (await ctx.Insert(peopleToInsert)).ShouldBe(3);

                var peopleInserted = (await ctx.Get()).ToArray();
                peopleInserted.Length.ShouldBe(3);

                peopleInserted[0].SomeId.ShouldBe(1);

                var personToUpdate = new MyPerson
                {
                    SomeId = 0,
                    Age = 100
                };

                var filter = ctx.Query.Filter.And(p => p.SomeId, Operator.Equal, 0);
                Should.Throw<SqlException>(async () => await ctx.UpdateWhere(personToUpdate, filter))
                    .Message.ShouldBe("Cannot update identity column 'Id'.");

                filter = ctx.Query.Filter.And(p => p.SomeId, Operator.Equal, peopleInserted[0].SomeId);

                Should.Throw<SqlException>(async () => await ctx.UpdateWhere(personToUpdate, filter))
                    .Message.ShouldBe("Cannot update identity column 'Id'.");

                personToUpdate.SomeName = peopleInserted[0].SomeName;

                Should.Throw<SqlException>(async () => await ctx.UpdateWhere(personToUpdate, filter))
                    .Message.ShouldBe("Cannot update identity column 'Id'.");

                Should.Throw<InvalidDataException>(async () => await ctx.UpdatePartialWhere(new { }, filter))
                    .Message.ShouldBe("Unable to find any properties in: item");

                var anotherPersonToUpdate = new MyPerson
                {
                    SomeId = 666,
                    Age = 66,
                    SomeName = "P3 - Updated"
                };

                filter = ctx.Query.Filter.And(p => p.SomeId, Operator.Equal, 3);

                Should.Throw<SqlException>(async () => await ctx.UpdateWhere(anotherPersonToUpdate, filter))
                    .Message.ShouldBe("Cannot update identity column 'Id'.");

                (await ctx.Count(p => p.SomeId)).ShouldBe((ulong)3);
                (await ctx.Count(p => p.SomeId, ctx.Query.Filter.And(p => p.SomeId, Operator.Equal, 3))).ShouldBe((ulong)1);
            }
        }

        private static async Task When_partially_updating_non_aliased_model()
        {
            /*
             * Updating with a partial item should update ALL the columns
             * for the item AND the id/filter for the records to be updated should be supplied
             */
            using (var conn = new SqlConnection(ConnectionString))
            {
                var ctx = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await ctx.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await ctx.Count(p => p.Id)).ShouldBe((ulong)0);

                var peopleToInsert = new[]
                {
                    new Person{ Age = 10, Name = "P1"},
                    new Person{ Age = 20, Name = "P2"},
                    new Person{ Age = 30, Name = "P3"}
                };

                (await ctx.Insert(peopleToInsert)).ShouldBe(3);

                var peopleInserted = (await ctx.Get()).ToArray();
                peopleInserted.Length.ShouldBe(3);

                peopleInserted[0].Id.ShouldBe(1);

                var personToUpdate = new
                {
                    Age = 100
                };

                var filter = ctx.Query.Filter.And(p => p.Id, Operator.Equal, 0);

                (await ctx.UpdatePartialWhere(personToUpdate, filter)).ShouldBe(0);

                filter = ctx.Query.Filter.And(p => p.Id, Operator.Equal, 1);
                (await ctx.UpdatePartialWhere(personToUpdate, filter)).ShouldBe(1);

                (await ctx.GetWhere(filter)).Single().Id.ShouldBe(1);
                (await ctx.GetWhere(filter)).Single().Age.ShouldBe(100);
                (await ctx.GetWhere(filter)).Single().Name.ShouldBe("P1");

                var anotherPersonToUpdate = new Person
                {
                    Age = 66,
                    Name = "P3 - Updated"
                };

                filter = ctx.Query.Filter.And(p => p.Id, Operator.Equal, 3);
                Should.Throw<SqlException>(async () => await ctx.UpdateWhere(anotherPersonToUpdate, filter))
                    .Message.ShouldBe("Cannot update identity column 'Id'.");

                Should.Throw<InvalidDataException>(async () => await ctx.UpdatePartialWhere(new { }, filter))
                    .Message.ShouldBe("Unable to find any properties in: item");

                dynamic yetAnotherPersonToUpdate = new ExpandoObject();
                yetAnotherPersonToUpdate.Age = 555;
                yetAnotherPersonToUpdate.Name = "P2 - Updated";

                filter = ctx.Query.Filter.And(p => p.Id, Operator.Equal, 2);
                ((int)await ctx.UpdatePartialWhere(yetAnotherPersonToUpdate, filter)).ShouldBe(1);

                (await ctx.GetWhere(p => p.Id, 2)).Single().Id.ShouldBe(2);
                (await ctx.GetWhere(p => p.Id, 2)).Single().Age.ShouldBe(555);
                (await ctx.GetWhere(p => p.Id, 2)).Single().Name.ShouldBe("P2 - Updated");

                filter = ctx.Query.Filter.And(p => p.Id, Operator.Equal, 3);
                (await ctx.Count(p => p.Id, filter)).ShouldBe((ulong)1);
                (await ctx.Count(p => p.Id)).ShouldBe((ulong)3);
            }
        }

        private static async Task When_partially_updating_aliased_model()
        {
            /*
             * Updating with a partial item should update ALL the columns
             * for the item AND the id/filter for the records to be updated should be supplied
             */
            using (var conn = new SqlConnection(ConnectionString))
            {
                var ctx = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await ctx.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await ctx.Count(p => p.SomeId)).ShouldBe((ulong)0);

                var peopleToInsert = new[]
                {
                    new MyPerson{ Age = 10, SomeName = "P1"},
                    new MyPerson{ Age = 20, SomeName = "P2"},
                    new MyPerson{ Age = 30, SomeName = "P3"}
                };

                (await ctx.Insert(peopleToInsert)).ShouldBe(3);

                var peopleInserted = (await ctx.Get()).ToArray();
                peopleInserted.Length.ShouldBe(3);

                peopleInserted[0].SomeId.ShouldBe(1);

                var personToUpdate = new
                {
                    Age = 100
                };

                var filter = ctx.Query.Filter.And(p => p.SomeId, Operator.Equal, 0);

                (await ctx.UpdatePartialWhere(personToUpdate, filter)).ShouldBe(0);

                filter = ctx.Query.Filter.And(p => p.SomeId, Operator.Equal, 1);
                (await ctx.UpdatePartialWhere(personToUpdate, filter)).ShouldBe(1);

                (await ctx.GetWhere(filter)).Single().SomeId.ShouldBe(1);
                (await ctx.GetWhere(filter)).Single().Age.ShouldBe(100);
                (await ctx.GetWhere(filter)).Single().SomeName.ShouldBe("P1");

                var anotherPersonToUpdate = new MyPerson
                {
                    Age = 66,
                    SomeName = "P3 - Updated"
                };

                filter = ctx.Query.Filter.And(p => p.SomeId, Operator.Equal, 3);
                Should.Throw<SqlException>(async () => await ctx.UpdateWhere(anotherPersonToUpdate, filter))
                    .Message.ShouldBe("Cannot update identity column 'Id'.");

                dynamic yetAnotherPersonToUpdate = new ExpandoObject();
                yetAnotherPersonToUpdate.Age = 555;
                yetAnotherPersonToUpdate.SomeName = "P2 - Updated";

                filter = ctx.Query.Filter.And(p => p.SomeId, Operator.Equal, 2);
                ((int)await ctx.UpdatePartialWhere(yetAnotherPersonToUpdate, filter)).ShouldBe(1);

                (await ctx.GetWhere(p => p.SomeId, 2)).Single().SomeId.ShouldBe(2);
                (await ctx.GetWhere(p => p.SomeId, 2)).Single().Age.ShouldBe(555);
                (await ctx.GetWhere(p => p.SomeId, 2)).Single().SomeName.ShouldBe("P2 - Updated");

                filter = ctx.Query.Filter.And(p => p.SomeId, Operator.Equal, 3);
                (await ctx.Count(p => p.SomeId, filter)).ShouldBe((ulong)1);
                (await ctx.Count(p => p.SomeId)).ShouldBe((ulong)3);
            }
        }

        private static async Task When_updating_single_by_id_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var p1 = new Person { Name = "P1", Age = 10 };
                ((long)await repo.Insert(p1)).ShouldBe(1);

                var insertedP1 = (await repo.Get()).Single();
                insertedP1.ShouldNotBeNull();
                insertedP1.Id.ShouldBe(1);
                insertedP1.Name.ShouldBe("P1");
                insertedP1.Age.ShouldBe(10);

                var p2 = new Person { Name = "P2", Age = 20 };
                ((long)await repo.Insert(p2)).ShouldBe(2);

                var insertedP2 = (await repo.GetWhere(p => p.Id, 2)).Single();
                insertedP2.ShouldNotBeNull();
                insertedP2.Id.ShouldBe(2);
                insertedP2.Name.ShouldBe("P2");
                insertedP2.Age.ShouldBe(20);

                (await repo.Get()).Count.ShouldBe(2);

                var updateToP1 = insertedP1;
                updateToP1.Name = "P1-updated";
                updateToP1.Age = 60;

                (await repo.Update(updateToP1)).ShouldBe(1);

                (await repo.Get()).Count.ShouldBe(2);

                var updatedP1 = (await repo.GetWhere(p => p.Id, updateToP1.Id)).Single();
                updatedP1.ShouldNotBeNull();
                updatedP1.Id.ShouldBe(updateToP1.Id);
                updatedP1.Name.ShouldBe("P1-updated");
                updatedP1.Age.ShouldBe(60);

                var nonExistingPerson = new Person { Id = 1234, Name = "Santa", Age = 96 };
                (await repo.Update(nonExistingPerson)).ShouldBe(0);

                (await repo.Get()).Count.ShouldBe(2);
            }
        }

        private static async Task When_updating_single_by_id_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var p1 = new MyPerson { SomeName = "P1", Age = 10 };
                ((long)await repo.Insert(p1)).ShouldBe(1);

                var insertedP1 = (await repo.Get()).Single();
                insertedP1.ShouldNotBeNull();
                insertedP1.SomeId.ShouldBe(1);
                insertedP1.SomeName.ShouldBe("P1");
                insertedP1.Age.ShouldBe(10);

                var p2 = new MyPerson { SomeName = "P2", Age = 20 };
                ((long)await repo.Insert(p2)).ShouldBe(2);

                var insertedP2 = (await repo.GetWhere(p => p.SomeId, 2)).Single();
                insertedP2.ShouldNotBeNull();
                insertedP2.SomeId.ShouldBe(2);
                insertedP2.SomeName.ShouldBe("P2");
                insertedP2.Age.ShouldBe(20);

                (await repo.Get()).Count.ShouldBe(2);

                var updateToP1 = insertedP1;
                updateToP1.SomeName = "P1-updated";
                updateToP1.Age = 60;

                (await repo.Update(updateToP1)).ShouldBe(1);

                (await repo.Get()).Count.ShouldBe(2);

                var updatedP1 = (await repo.GetWhere(p => p.SomeId, updateToP1.SomeId)).Single();
                updatedP1.ShouldNotBeNull();
                updatedP1.SomeId.ShouldBe(updateToP1.SomeId);
                updatedP1.SomeName.ShouldBe("P1-updated");
                updatedP1.Age.ShouldBe(60);

                var nonExistingPerson = new MyPerson { SomeId = 1234, SomeName = "Santa", Age = 96 };
                (await repo.Update(nonExistingPerson)).ShouldBe(0);

                (await repo.Get()).Count.ShouldBe(2);
            }
        }

        private static async Task When_updating_custom_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var ctx = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await ctx.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await ctx.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 30 },
                    new Person { Name = "P5", Age = 50 },
                    new Person { Name = "P5", Age = 60 },
                    new Person { Name = "P5", Age = 70 }
                };

                (await ctx.Insert(people)).ShouldBe(7);

                var insertedPeople = (await ctx.Get()).ToArray();

                var template1 = new { Name = "P0", Age = 0 };
                var filter = ctx.Query.Filter.And(p => p.Age, Operator.Equal, 10);
                (await ctx.UpdatePartialWhere(template1, filter)).ShouldBe(1);

                var snapshot1 = (await ctx.Get()).ToArray();
                snapshot1.Length.ShouldBe(insertedPeople.Length);

                snapshot1[0].Id.ShouldBe(1);
                snapshot1[0].Name.ShouldBe("P0");
                snapshot1[0].Age.ShouldBe(0);

                for (var i = 1; i < insertedPeople.Length; i++)
                {
                    snapshot1[i].Id.ShouldBe(insertedPeople[i].Id);
                    snapshot1[i].Name.ShouldBe(insertedPeople[i].Name);
                    snapshot1[i].Age.ShouldBe(insertedPeople[i].Age);
                }

                var template2 = new { Name = "P100", Age = 100 };
                filter = ctx.Query.Filter.And(p => p.Name, Operator.Equal, "P5");
                (await ctx.UpdatePartialWhere(template2, filter)).ShouldBe(3);

                var snapshot2 = (await ctx.Get()).ToArray();
                snapshot2.Length.ShouldBe(insertedPeople.Length);

                for (var i = snapshot1.Length - 4; i >= 0; i--)
                {
                    snapshot2[i].Id.ShouldBe(snapshot1[i].Id);
                    snapshot2[i].Name.ShouldBe(snapshot1[i].Name);
                    snapshot2[i].Age.ShouldBe(snapshot1[i].Age);
                }

                snapshot2[4].Id.ShouldBe(5);
                snapshot2[4].Name.ShouldBe("P100");
                snapshot2[4].Age.ShouldBe(100);

                snapshot2[5].Id.ShouldBe(6);
                snapshot2[5].Name.ShouldBe("P100");
                snapshot2[5].Age.ShouldBe(100);

                snapshot2[6].Id.ShouldBe(7);
                snapshot2[6].Name.ShouldBe("P100");
                snapshot2[6].Age.ShouldBe(100);

                var template3 = new { Name = "P200", Age = 200 };
                filter = ctx.Query.Filter.AndIn(p => p.Id, new long[] { 1, 2, 3 });
                (await ctx.UpdatePartialWhere(template3, filter)).ShouldBe(3);

                var snapshot3 = (await ctx.Get()).ToArray();
                snapshot3.Length.ShouldBe(insertedPeople.Length);

                snapshot3[0].Id.ShouldBe(1);
                snapshot3[0].Name.ShouldBe("P200");
                snapshot3[0].Age.ShouldBe(200);

                snapshot3[1].Id.ShouldBe(2);
                snapshot3[1].Name.ShouldBe("P200");
                snapshot3[1].Age.ShouldBe(200);

                snapshot3[2].Id.ShouldBe(3);
                snapshot3[2].Name.ShouldBe("P200");
                snapshot3[2].Age.ShouldBe(200);

                for (var i = 3; i < snapshot2.Length; i++)
                {
                    snapshot3[i].Id.ShouldBe(snapshot2[i].Id);
                    snapshot3[i].Name.ShouldBe(snapshot2[i].Name);
                    snapshot3[i].Age.ShouldBe(snapshot2[i].Age);
                }
            }
        }

        private static async Task When_updating_custom_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var ctx = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await ctx.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await ctx.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 30 },
                    new MyPerson { SomeName = "P5", Age = 50 },
                    new MyPerson { SomeName = "P5", Age = 60 },
                    new MyPerson { SomeName = "P5", Age = 70 }
                };

                (await ctx.Insert(people)).ShouldBe(7);

                var insertedPeople = (await ctx.Get()).ToArray();

                var template1 = new { SomeName = "P0", Age = 0 };
                var filter = ctx.Query.Filter.And(p => p.Age, Operator.Equal, 10);
                (await ctx.UpdatePartialWhere(template1, filter)).ShouldBe(1);

                var snapshot1 = (await ctx.Get()).ToArray();
                snapshot1.Length.ShouldBe(insertedPeople.Length);

                snapshot1[0].SomeId.ShouldBe(1);
                snapshot1[0].SomeName.ShouldBe("P0");
                snapshot1[0].Age.ShouldBe(0);

                for (var i = 1; i < insertedPeople.Length; i++)
                {
                    snapshot1[i].SomeId.ShouldBe(insertedPeople[i].SomeId);
                    snapshot1[i].SomeName.ShouldBe(insertedPeople[i].SomeName);
                    snapshot1[i].Age.ShouldBe(insertedPeople[i].Age);
                }

                var template2 = new { SomeName = "P100", Age = 100 };
                filter = ctx.Query.Filter.And(p => p.SomeName, Operator.Equal, "P5");
                (await ctx.UpdatePartialWhere(template2, filter)).ShouldBe(3);

                var snapshot2 = (await ctx.Get()).ToArray();
                snapshot2.Length.ShouldBe(insertedPeople.Length);

                for (var i = snapshot1.Length - 4; i >= 0; i--)
                {
                    snapshot2[i].SomeId.ShouldBe(snapshot1[i].SomeId);
                    snapshot2[i].SomeName.ShouldBe(snapshot1[i].SomeName);
                    snapshot2[i].Age.ShouldBe(snapshot1[i].Age);
                }

                snapshot2[4].SomeId.ShouldBe(5);
                snapshot2[4].SomeName.ShouldBe("P100");
                snapshot2[4].Age.ShouldBe(100);

                snapshot2[5].SomeId.ShouldBe(6);
                snapshot2[5].SomeName.ShouldBe("P100");
                snapshot2[5].Age.ShouldBe(100);

                snapshot2[6].SomeId.ShouldBe(7);
                snapshot2[6].SomeName.ShouldBe("P100");
                snapshot2[6].Age.ShouldBe(100);

                var template3 = new { SomeName = "P200", Age = 200 };
                filter = ctx.Query.Filter.AndIn(p => p.SomeId, new long[] { 1, 2, 3 });
                (await ctx.UpdatePartialWhere(template3, filter)).ShouldBe(3);

                var snapshot3 = (await ctx.Get()).ToArray();
                snapshot3.Length.ShouldBe(insertedPeople.Length);

                snapshot3[0].SomeId.ShouldBe(1);
                snapshot3[0].SomeName.ShouldBe("P200");
                snapshot3[0].Age.ShouldBe(200);

                snapshot3[1].SomeId.ShouldBe(2);
                snapshot3[1].SomeName.ShouldBe("P200");
                snapshot3[1].Age.ShouldBe(200);

                snapshot3[2].SomeId.ShouldBe(3);
                snapshot3[2].SomeName.ShouldBe("P200");
                snapshot3[2].Age.ShouldBe(200);

                for (var i = 3; i < snapshot2.Length; i++)
                {
                    snapshot3[i].SomeId.ShouldBe(snapshot2[i].SomeId);
                    snapshot3[i].SomeName.ShouldBe(snapshot2[i].SomeName);
                    snapshot3[i].Age.ShouldBe(snapshot2[i].Age);
                }
            }
        }

        private static async Task When_updating_multiple_non_aliased_models()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person {Name = "P1", Age = 10},
                    new Person {Name = "P2", Age = 10},
                    new Person {Name = "P3", Age = 20}
                };

                (await repo.Insert(people)).ShouldBe(3);

                var existing = (await repo.Get()).OrderBy(x => x.Id).ToArray();
                existing.Length.ShouldBe(3);

                var toUpdate = new[]
                {
                    new Person {Name = "P1-updated", Age = 12},
                    new Person {Name = "P2-updated", Age = 10}
                };

                toUpdate[0].Id = existing[0].Id;
                toUpdate[1].Id = existing[1].Id;

                (await repo.Update(toUpdate)).ShouldBe(2);

                var finalItems = (await repo.Get()).OrderBy(x => x.Id).ToArray();

                finalItems.Length.ShouldBe(3);

                finalItems[0].Id.ShouldBe(toUpdate[0].Id);
                finalItems[0].Name.ShouldBe(toUpdate[0].Name);
                finalItems[0].Age.ShouldBe(toUpdate[0].Age);

                finalItems[1].Id.ShouldBe(toUpdate[1].Id);
                finalItems[1].Name.ShouldBe(toUpdate[1].Name);
                finalItems[1].Age.ShouldBe(toUpdate[1].Age);

                finalItems[2].Id.ShouldBe(existing[2].Id);
                finalItems[2].Name.ShouldBe(existing[2].Name);
                finalItems[2].Age.ShouldBe(existing[2].Age);
            }
        }

        private static async Task When_updating_multiple_aliased_models()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson {SomeName = "P1", Age = 10},
                    new MyPerson {SomeName = "P2", Age = 10},
                    new MyPerson {SomeName = "P3", Age = 20}
                };

                (await repo.Insert(people)).ShouldBe(3);

                var existing = (await repo.Get()).OrderBy(x => x.SomeId).ToArray();
                existing.Length.ShouldBe(3);

                var toUpdate = new[]
                {
                    new MyPerson {SomeName = "P1-updated", Age = 12},
                    new MyPerson {SomeName = "P2-updated", Age = 10}
                };

                toUpdate[0].SomeId = existing[0].SomeId;
                toUpdate[1].SomeId = existing[1].SomeId;

                (await repo.Update(toUpdate)).ShouldBe(2);

                var finalItems = (await repo.Get()).OrderBy(x => x.SomeId).ToArray();

                finalItems.Length.ShouldBe(3);

                finalItems[0].SomeId.ShouldBe(toUpdate[0].SomeId);
                finalItems[0].SomeName.ShouldBe(toUpdate[0].SomeName);
                finalItems[0].Age.ShouldBe(toUpdate[0].Age);

                finalItems[1].SomeId.ShouldBe(toUpdate[1].SomeId);
                finalItems[1].SomeName.ShouldBe(toUpdate[1].SomeName);
                finalItems[1].Age.ShouldBe(toUpdate[1].Age);

                finalItems[2].SomeId.ShouldBe(existing[2].SomeId);
                finalItems[2].SomeName.ShouldBe(existing[2].SomeName);
                finalItems[2].Age.ShouldBe(existing[2].Age);
            }
        }

        private static async Task When_deleting_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                await conn.ExecuteAsync(DefaultTableQuery);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 30 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                var insertedPeople = (await repo.Get()).ToArray();
                insertedPeople.Length.ShouldBe(people.Length);

                (await repo.GetWhere(p => p.Id, 1)).ShouldNotBeEmpty();
                (await repo.DeleteWhere(p => p.Id, 1)).ShouldBe(1);
                (await repo.Get()).Count.ShouldBe(3);
                (await repo.GetWhere(p => p.Id, 1)).ShouldBeEmpty();

                (await repo.GetWhere(p => p.Age, 30)).Count.ShouldBe(2);
                (await repo.DeleteWhere(p => p.Age, 30)).ShouldBe(2);
                (await repo.Get()).Count.ShouldBe(1);
                (await repo.GetWhere(p => p.Age, 30)).ShouldBeEmpty();

                var remainingPeople = (await repo.Get()).Single();
                remainingPeople.Id.ShouldBe(2);
                remainingPeople.Name.ShouldBe("P2");
                remainingPeople.Age.ShouldBe(20);

                var morePeople = new[]
                {
                    new Person { Name = "MP1", Age = 50 },
                    new Person { Name = "MP2", Age = 60 },
                    new Person { Name = "MP3", Age = 70 },
                    new Person { Name = "MP4", Age = 80 },
                    new Person { Name = "MP5", Age = 90 },
                    new Person { Name = "MP5", Age = 100 }
                };

                (await repo.Insert(morePeople)).ShouldBe(6);

                var allPeople = (await repo.Get()).ToArray();
                allPeople.Length.ShouldBe(morePeople.Length + 1);

                allPeople[0].Id.ShouldBe(remainingPeople.Id);
                allPeople[0].Name.ShouldBe(remainingPeople.Name);
                allPeople[0].Age.ShouldBe(remainingPeople.Age);
                (await repo.DeleteWhere(p => p.Id, null, remainingPeople.Id, allPeople[1].Id)).ShouldBe(2);

                (await repo.Get()).Count.ShouldBe(morePeople.Length + 1 - 2);

                (await repo.DeleteWhere(p => p.Name, null, "MP4", "MP5")).ShouldBe(3);

                var totalPeopleRemaining = (await repo.Get()).ToArray();
                totalPeopleRemaining.Length.ShouldBe(2);

                totalPeopleRemaining[0].Id.ShouldBe(allPeople[2].Id);
                totalPeopleRemaining[0].Name.ShouldBe(allPeople[2].Name);
                totalPeopleRemaining[0].Age.ShouldBe(allPeople[2].Age);

                totalPeopleRemaining[1].Id.ShouldBe(allPeople[3].Id);
                totalPeopleRemaining[1].Name.ShouldBe(allPeople[3].Name);
                totalPeopleRemaining[1].Age.ShouldBe(allPeople[3].Age);
            }
        }

        private static async Task When_deleting_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 30 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                var insertedPeople = (await repo.Get()).ToArray();
                insertedPeople.Length.ShouldBe(people.Length);

                (await repo.GetWhere(p => p.SomeId, 1)).ShouldNotBeEmpty();
                (await repo.DeleteWhere(p => p.SomeId, 1)).ShouldBe(1);
                (await repo.Get()).Count.ShouldBe(3);
                (await repo.GetWhere(p => p.SomeId, 1)).ShouldBeEmpty();

                (await repo.GetWhere(p => p.Age, 30)).Count.ShouldBe(2);
                (await repo.DeleteWhere(p => p.Age, 30)).ShouldBe(2);
                (await repo.Get()).Count.ShouldBe(1);
                (await repo.GetWhere(p => p.Age, 30)).ShouldBeEmpty();

                var remainingPeople = (await repo.Get()).Single();
                remainingPeople.SomeId.ShouldBe(2);
                remainingPeople.SomeName.ShouldBe("P2");
                remainingPeople.Age.ShouldBe(20);

                var morePeople = new[]
                {
                    new MyPerson { SomeName = "MP1", Age = 50 },
                    new MyPerson { SomeName = "MP2", Age = 60 },
                    new MyPerson { SomeName = "MP3", Age = 70 },
                    new MyPerson { SomeName = "MP4", Age = 80 },
                    new MyPerson { SomeName = "MP5", Age = 90 },
                    new MyPerson { SomeName = "MP5", Age = 100 }
                };

                (await repo.Insert(morePeople)).ShouldBe(6);

                var allPeople = (await repo.Get()).ToArray();
                allPeople.Length.ShouldBe(morePeople.Length + 1);

                allPeople[0].SomeId.ShouldBe(remainingPeople.SomeId);
                allPeople[0].SomeName.ShouldBe(remainingPeople.SomeName);
                allPeople[0].Age.ShouldBe(remainingPeople.Age);
                (await repo.DeleteWhere(p => p.SomeId, null, remainingPeople.SomeId, allPeople[1].SomeId)).ShouldBe(2);

                (await repo.Get()).Count.ShouldBe(morePeople.Length + 1 - 2);

                (await repo.DeleteWhere(p => p.SomeName, null, "MP4", "MP5")).ShouldBe(3);

                var totalPeopleRemaining = (await repo.Get()).ToArray();
                totalPeopleRemaining.Length.ShouldBe(2);

                totalPeopleRemaining[0].SomeId.ShouldBe(allPeople[2].SomeId);
                totalPeopleRemaining[0].SomeName.ShouldBe(allPeople[2].SomeName);
                totalPeopleRemaining[0].Age.ShouldBe(allPeople[2].Age);

                totalPeopleRemaining[1].SomeId.ShouldBe(allPeople[3].SomeId);
                totalPeopleRemaining[1].SomeName.ShouldBe(allPeople[3].SomeName);
                totalPeopleRemaining[1].Age.ShouldBe(allPeople[3].Age);
            }
        }

        private static async Task When_deleting_all_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();
                (await repo.DeleteAll()).ShouldBe(0);

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 30 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                var insertedPeople = (await repo.Get()).ToArray();
                insertedPeople.Length.ShouldBe(people.Length);

                (await repo.DeleteAll()).ShouldBe(insertedPeople.Length);
                (await repo.Get()).ShouldBeEmpty();
            }
        }

        private static async Task When_deleting_all_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);
                (await repo.Get()).ShouldBeEmpty();
                (await repo.DeleteAll()).ShouldBe(0);

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 30 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                var insertedPeople = (await repo.Get()).ToArray();
                insertedPeople.Length.ShouldBe(people.Length);

                (await repo.DeleteAll()).ShouldBe(insertedPeople.Length);
                (await repo.Get()).ShouldBeEmpty();
            }
        }

        private static async Task When_counting_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);
                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);

                (await repo.Count(p => p.Id)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Age)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Name)).ShouldBe((ulong)0);

                (await repo.Count(p => p.Id, true)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Age, true)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Name, true)).ShouldBe((ulong)0);

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 30 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P3", Age = 30 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                (await repo.Count(p => p.Id)).ShouldBe((ulong)people.Length);
                (await repo.Count(p => p.Age)).ShouldBe((ulong)people.Length);
                (await repo.Count(p => p.Name)).ShouldBe((ulong)people.Length);

                (await repo.Count(p => p.Id, true)).ShouldBe((ulong)people.Length);
                (await repo.Count(p => p.Age, true)).ShouldBe((ulong)2);
                (await repo.Count(p => p.Name, true)).ShouldBe((ulong)3);

                await repo.DeleteAll();

                (await repo.Count(p => p.Id)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Age)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Name)).ShouldBe((ulong)0);

                (await repo.Count(p => p.Id, true)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Age, true)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Name, true)).ShouldBe((ulong)0);
            }
        }

        private static async Task When_counting_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);
                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);

                (await repo.Count(p => p.SomeId)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Age)).ShouldBe((ulong)0);
                (await repo.Count(p => p.SomeName)).ShouldBe((ulong)0);

                (await repo.Count(p => p.SomeId, true)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Age, true)).ShouldBe((ulong)0);
                (await repo.Count(p => p.SomeName, true)).ShouldBe((ulong)0);

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 30 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P3", Age = 30 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                (await repo.Count(p => p.SomeId)).ShouldBe((ulong)people.Length);
                (await repo.Count(p => p.Age)).ShouldBe((ulong)people.Length);
                (await repo.Count(p => p.SomeName)).ShouldBe((ulong)people.Length);

                (await repo.Count(p => p.SomeId, true)).ShouldBe((ulong)people.Length);
                (await repo.Count(p => p.Age, true)).ShouldBe((ulong)2);
                (await repo.Count(p => p.SomeName, true)).ShouldBe((ulong)3);

                await repo.DeleteAll();

                (await repo.Count(p => p.SomeId)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Age)).ShouldBe((ulong)0);
                (await repo.Count(p => p.SomeName)).ShouldBe((ulong)0);

                (await repo.Count(p => p.SomeId, true)).ShouldBe((ulong)0);
                (await repo.Count(p => p.Age, true)).ShouldBe((ulong)0);
                (await repo.Count(p => p.SomeName, true)).ShouldBe((ulong)0);
            }
        }

        private static async Task When_min_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Min(p => p.Id)).ShouldBe(0);
                (await repo.Min(p => p.Age)).ShouldBe(0);
                (await repo.Min(p => p.Name)).ShouldBeNull();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 30 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P3", Age = 30 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                (await repo.Min(p => p.Id)).ShouldBe(1);
                (await repo.Min(p => p.Age)).ShouldBe(10);
                (await repo.Min(p => p.Name)).ShouldBe("P1");
            }
        }

        private static async Task When_min_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Min(p => p.SomeId)).ShouldBe(0);
                (await repo.Min(p => p.Age)).ShouldBe(0);
                (await repo.Min(p => p.SomeName)).ShouldBeNull();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 30 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P3", Age = 30 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                (await repo.Min(p => p.SomeId)).ShouldBe(1);
                (await repo.Min(p => p.Age)).ShouldBe(10);
                (await repo.Min(p => p.SomeName)).ShouldBe("P1");
            }
        }

        private static async Task When_max_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Max(p => p.Id)).ShouldBe(0);
                (await repo.Max(p => p.Age)).ShouldBe(0);
                (await repo.Max(p => p.Name)).ShouldBeNull();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 30 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P3", Age = 30 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                (await repo.Max(p => p.Id)).ShouldBe(4);
                (await repo.Max(p => p.Age)).ShouldBe(30);
                (await repo.Max(p => p.Name)).ShouldBe("P3");
            }
        }

        private static async Task When_max_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Max(p => p.SomeId)).ShouldBe(0);
                (await repo.Max(p => p.Age)).ShouldBe(0);
                (await repo.Max(p => p.SomeName)).ShouldBeNull();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 30 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P3", Age = 30 }
                };

                (await repo.Insert(people)).ShouldBe(4);

                (await repo.Max(p => p.SomeId)).ShouldBe(4);
                (await repo.Max(p => p.Age)).ShouldBe(30);
                (await repo.Max(p => p.SomeName)).ShouldBe("P3");
            }
        }

        private static async Task When_sum_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 40 },
                    new Person { Name = "P5", Age = 10 }
                };

                (await repo.Insert(people)).ShouldBe(5);

                var insertedPeople = (await repo.Get()).ToArray();

                insertedPeople.Length.ShouldBe(5);

                (await repo.Sum(p => p.Id)).ShouldBe(15);
                (await repo.Sum(p => p.Age)).ShouldBe(110);

                (await repo.Sum(p => p.Id, true)).ShouldBe(15);
                (await repo.Sum(p => p.Age, true)).ShouldBe(100);

                Should.Throw<SqlException>(async () => await repo.Sum(p => p.Name))
                    .Message.ShouldBe("Operand data type nvarchar is invalid for sum operator.");
                Should.Throw<SqlException>(async () => await repo.Sum(p => p.Name, true))
                    .Message.ShouldBe("Operand data type nvarchar is invalid for sum operator.");
            }
        }

        private static async Task When_sum_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 40 },
                    new MyPerson { SomeName = "P5", Age = 10 }
                };

                (await repo.Insert(people)).ShouldBe(5);

                var insertedPeople = (await repo.Get()).ToArray();

                insertedPeople.Length.ShouldBe(5);

                (await repo.Sum(p => p.SomeId)).ShouldBe(15);
                (await repo.Sum(p => p.Age)).ShouldBe(110);

                (await repo.Sum(p => p.SomeId, true)).ShouldBe(15);
                (await repo.Sum(p => p.Age, true)).ShouldBe(100);

                Should.Throw<SqlException>(async () => await repo.Sum(p => p.SomeName))
                    .Message.ShouldBe("Operand data type nvarchar is invalid for sum operator.");
                Should.Throw<SqlException>(async () => await repo.Sum(p => p.SomeName, true))
                    .Message.ShouldBe("Operand data type nvarchar is invalid for sum operator.");
            }
        }

        private static async Task When_avg_non_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 40 },
                    new Person { Name = "P5", Age = 50 },
                    new Person { Name = "P6", Age = 60 },
                    new Person { Name = "P7", Age = 10 }
                };

                (await repo.Insert(people)).ShouldBe(7);

                var insertedPeople = (await repo.Get()).ToArray();

                insertedPeople.Length.ShouldBe(7);

                (await repo.Sum(p => p.Id)).ShouldBe(28);
                (await repo.Sum(p => p.Age)).ShouldBe(220);

                (await repo.Avg(p => p.Id)).ShouldBe(4m);
                (await repo.Avg(p => p.Age)).ShouldBe(31);

                (await repo.Avg(p => p.Id, true)).ShouldBe(4m);
                (await repo.Avg(p => p.Age, true)).ShouldBe(35m);

                Should.Throw<SqlException>(async () => await repo.Sum(p => p.Name))
                    .Message.ShouldBe("Operand data type nvarchar is invalid for sum operator.");
                Should.Throw<SqlException>(async () => await repo.Sum(p => p.Name, true))
                    .Message.ShouldBe("Operand data type nvarchar is invalid for sum operator.");
            }
        }

        private static async Task When_avg_aliased_model()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<MyPerson>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                (await repo.Get()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 40 },
                    new MyPerson { SomeName = "P5", Age = 50 },
                    new MyPerson { SomeName = "P6", Age = 60 },
                    new MyPerson { SomeName = "P7", Age = 10 }
                };

                (await repo.Insert(people)).ShouldBe(7);

                var insertedPeople = (await repo.Get()).ToArray();

                insertedPeople.Length.ShouldBe(7);

                (await repo.Sum(p => p.SomeId)).ShouldBe(28);
                (await repo.Sum(p => p.Age)).ShouldBe(220);

                (await repo.Avg(p => p.SomeId)).ShouldBe(4m);
                (await repo.Avg(p => p.Age)).ShouldBe(31);

                (await repo.Avg(p => p.SomeId, true)).ShouldBe(4m);
                (await repo.Avg(p => p.Age, true)).ShouldBe(35m);

                Should.Throw<SqlException>(async () => await repo.Sum(p => p.SomeName))
                    .Message.ShouldBe("Operand data type nvarchar is invalid for sum operator.");
                Should.Throw<SqlException>(async () => await repo.Sum(p => p.SomeName, true))
                    .Message.ShouldBe("Operand data type nvarchar is invalid for sum operator.");
            }
        }

        private static async Task When_doing_multiple_operations_with_sample_model()
        {
            // ReSharper disable once InconsistentNaming
            const string tableQuery = @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='SampleModel' AND xtype='U')
CREATE TABLE SampleModel (
	Id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    [Text] VARCHAR(50) NOT NULL,
    [Int] INTEGER NOT NULL,
	[Decimal] DECIMAL(18,4) NOT NULL,
	[Double] REAL NOT NULL,
	[Float] FLOAT NOT NULL,
	[Flag] BIT NOT NULL,
	[Binary] BINARY(100) NOT NULL,
	[Key] UNIQUEIDENTIFIER NOT NULL,
	[DateTime] DATETIME2(7) NOT NULL,
	[DateTimeOffset] DATETIMEOFFSET(7) NOT NULL);";

            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(tableQuery);

                (await conn.Exists<SampleModel>()).ShouldBeTrue();

                await conn.ExecuteAsync("DELETE FROM SampleModel");
                await conn.ExecuteAsync("DBCC CHECKIDENT (SampleModel, RESEED, 0)");

                var binaryData = new byte[100];
                Encoding.UTF8.GetBytes("Hidden Message!").CopyTo(binaryData, 0);

                var sample1 = new SampleModel
                {
                    Text = "Model-1",
                    Int = 1,
                    Decimal = 1.1234m,
                    Double = 1.51,
                    Float = 1.9f,
                    Flag = true,
                    Binary = binaryData,
                    Guid = Guid.NewGuid(),
                    DateTime = new DateTime(1912, 1, 28, 10, 35, 11, 12),
                    DateTimeOffset = new DateTimeOffset(new DateTime(1912, 1, 28, 10, 35, 11, 12), TimeSpan.FromHours(2)),
                    Composite = null
                };

                var repo = conn.GetDBContext<SampleModel>(SQLServerDialect.Instance);

                ((long)await repo.Insert(sample1)).ShouldBe(1);

                var insertedSample1 = (await repo.Get()).Single();
                insertedSample1.Id.ShouldBe(1);
                insertedSample1.Int.ShouldBe(sample1.Int);
                insertedSample1.Decimal.ShouldBe(sample1.Decimal);
                insertedSample1.Double.ShouldBe(sample1.Double, 0.01d);
                insertedSample1.Float.ShouldBe(sample1.Float);
                insertedSample1.Flag.ShouldBe(sample1.Flag);
                insertedSample1.Binary.ShouldBe(sample1.Binary);
                insertedSample1.Guid.ShouldBe(sample1.Guid);
                insertedSample1.DateTime.ShouldBe(sample1.DateTime);

                insertedSample1.DateTimeOffset.Offset.ShouldBe(sample1.DateTimeOffset.Offset);
                insertedSample1.DateTimeOffset.DateTime.Kind.ShouldBe(sample1.DateTime.Kind);
                insertedSample1.DateTimeOffset.Date.ShouldBe(sample1.DateTimeOffset.Date);
                insertedSample1.DateTimeOffset.TimeOfDay.Hours.ShouldBe(sample1.DateTimeOffset.TimeOfDay.Hours);
                insertedSample1.DateTimeOffset.TimeOfDay.Minutes.ShouldBe(sample1.DateTimeOffset.TimeOfDay.Minutes);
                insertedSample1.DateTimeOffset.TimeOfDay.Seconds.ShouldBe(sample1.DateTimeOffset.TimeOfDay.Seconds);
                // [ToDo] - Make sure the bug is fixed in dapper - you can 
                // then use SetValue of the DateTimeOffsetHandler to store as file time or otherwise.
                // insertedSample1.DateTimeOffset.TimeOfDay.Milliseconds.ShouldBe(sample1.DateTimeOffset.TimeOfDay.Milliseconds);

                insertedSample1.Composite.ShouldBe(sample1.Composite);

                insertedSample1.Flag = true;
                insertedSample1.Double = 5.6;
                insertedSample1.Text = "Updated";

                (await repo.Update(insertedSample1)).ShouldBe(1);
                (await repo.Count(p => p.Id)).ShouldBe((uint)1);
                var updatedSample1 = (await repo.Get()).Single();

                updatedSample1.Id.ShouldBe(insertedSample1.Id);
                updatedSample1.Int.ShouldBe(insertedSample1.Int);
                updatedSample1.Decimal.ShouldBe(insertedSample1.Decimal);
                updatedSample1.Double.ShouldBe(insertedSample1.Double, 0.01d);
                updatedSample1.Float.ShouldBe(insertedSample1.Float);
                updatedSample1.Flag.ShouldBe(insertedSample1.Flag);
                updatedSample1.Binary.ShouldBe(insertedSample1.Binary);
                updatedSample1.Guid.ShouldBe(insertedSample1.Guid);
                updatedSample1.DateTime.ShouldBe(insertedSample1.DateTime);

                updatedSample1.DateTimeOffset.Offset.ShouldBe(insertedSample1.DateTimeOffset.Offset);
                updatedSample1.DateTimeOffset.DateTime.Kind.ShouldBe(insertedSample1.DateTime.Kind);
                updatedSample1.DateTimeOffset.Date.ShouldBe(insertedSample1.DateTimeOffset.Date);
                updatedSample1.DateTimeOffset.TimeOfDay.Hours.ShouldBe(insertedSample1.DateTimeOffset.TimeOfDay.Hours);
                updatedSample1.DateTimeOffset.TimeOfDay.Minutes.ShouldBe(insertedSample1.DateTimeOffset.TimeOfDay.Minutes);
                updatedSample1.DateTimeOffset.TimeOfDay.Seconds.ShouldBe(insertedSample1.DateTimeOffset.TimeOfDay.Seconds);
            }
        }

        private static async Task When_working_with_inheritted_model()
        {
            // ReSharper disable once InconsistentNaming
            const string tableQuery = @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Child' AND xtype='U')
CREATE TABLE Child (
	Id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL,
    [Name] VARCHAR(50) NOT NULL,
    [Age] INTEGER NOT NULL,
    [Toy] VARCHAR(50) NOT NULL,
	[PetName] VARCHAR(50) NOT NULL);";

            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(tableQuery);
                (await conn.Exists<Child>()).ShouldBeTrue();

                var repo = conn.GetDBContext<Child>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Child, RESEED, 0)");

                var child1 = new Child
                {
                    Id = 1,
                    Name = "Child-1",
                    Age = 10,
                    Pet = "Pet-1",
                    Toy = "Toy-1"
                };

                long insertedId1 = await repo.Insert(child1);
                insertedId1.ShouldBe(1);

                var retrievedChild1 = (await repo.GetWhere(c => c.Id, insertedId1)).First();
                retrievedChild1.Id.ShouldBe(1);
                retrievedChild1.Name.ShouldBe("Child-1");
                retrievedChild1.Age.ShouldBe(10);
                retrievedChild1.Pet.ShouldBe("Pet-1");
                retrievedChild1.Toy.ShouldBe("Toy-1");

                var child2 = new Child
                {
                    Id = 1,
                    Name = "Child-2",
                    Age = 20,
                    Pet = "Pet-2",
                    Toy = "Toy-2"
                };

                long insertedId2 = await repo.Insert(child2);
                insertedId2.ShouldBe(2);

                var retrievedChild2 = (await repo.GetWhere(c => c.Id, insertedId2)).First();
                retrievedChild2.Id.ShouldBe(2);
                retrievedChild2.Name.ShouldBe("Child-2");
                retrievedChild2.Age.ShouldBe(20);
                retrievedChild2.Pet.ShouldBe("Pet-2");
                retrievedChild2.Toy.ShouldBe("Toy-2");
            }
        }

        [Alias("PersonTemp")]
        private class PersonTempNoIdentity
        {
            [Key(false)]
            [Alias("Key")]
            public long Id { get; set; }

            public string Name { get; set; }
            public int Age { get; set; }
        }
    }
}