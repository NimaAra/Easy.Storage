namespace Easy.Storage.Tests.Unit.SQLServer
{
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading.Tasks;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.SQLServer;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class SQLServerStorageContextTranscationTests : Context
    {
        internal static async Task Run()
        {
            await When_querying_with_transaction();
            await When_inserting_with_transaction_commited();
            await When_inserting_with_transaction_rolled_back();
            await When_inserting_with_transaction_disposed();
            await When_inserting_multiple_with_transaction_disposed_one_inside_the_other_outside();
            await When_inserting_multiple_with_transaction_commited_one_inside_the_other_outside();
            await When_inserting_and_updating_with_transaction_commited();
            await When_inserting_and_updating_with_transaction_rolled_back();
            await When_inserting_and_updating_with_transaction_with_isolation_level_commited();
            await When_inserting_and_updating_with_transaction_with_isolation_level_rolled_back();
        }

        private static async Task When_querying_with_transaction()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                var person = new Person { Name = "P1", Age = 10 };
                
                await repo.Insert(person);

                conn.Open();
                var tran = repo.Connection.BeginTransaction();
                {
                    (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)1);
                }

                (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)1);
            }
        }

        private static async Task When_inserting_with_transaction_commited()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                var person = new Person { Name = "P1", Age = 10 };

                conn.Open();
                using (var tran = repo.Connection.BeginTransaction())
                {
                    await repo.Insert(person, transaction: tran);
                    (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)1);
                    tran.Commit();

                    (await repo.Count(p => p.Id)).ShouldBe((ulong)1);
                }

                (await repo.Count(p => p.Id)).ShouldBe((ulong)1);
            }
        }

        private static async Task When_inserting_with_transaction_rolled_back()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                var person = new Person { Name = "P1", Age = 10 };

                using (var tran = repo.Connection.BeginTransaction())
                {
                    await repo.Insert(person, transaction: tran);
                    (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)1);

                    (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)1);
                    tran.Rollback();
                    (await repo.Count(p => p.Id)).ShouldBe((ulong)0);
                }

                (await repo.Count(p => p.Id)).ShouldBe((ulong)0);
            }
        }

        private static async Task When_inserting_with_transaction_disposed()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                var person = new Person { Name = "P1", Age = 10 };

                using (var tran = repo.Connection.BeginTransaction())
                {
                    await repo.Insert(person, transaction: tran);
                    (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)1);
                    (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)1);
                }

                (await repo.Count(p => p.Id)).ShouldBe((ulong)0);
            }
        }

        private static async Task When_inserting_multiple_with_transaction_disposed_one_inside_the_other_outside()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                var person1 = new Person { Name = "P1", Age = 10 };
                await repo.Insert(person1);

                using (var tran = repo.Connection.BeginTransaction())
                {
                    var person2 = new Person { Name = "P2", Age = 20 };
                    await repo.Insert(person2, transaction: tran);
                    (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)2);
                    (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)2);
                }

                (await repo.Count(p => p.Id)).ShouldBe((ulong)1);
            }
        }

        private static async Task When_inserting_multiple_with_transaction_commited_one_inside_the_other_outside()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                var person1 = new Person { Name = "P1", Age = 10 };
                await repo.Insert(person1);

                using (var tran = repo.Connection.BeginTransaction())
                {
                    var person2 = new Person { Name = "P2", Age = 20 };
                    await repo.Insert(person2, transaction: tran);
                    (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)2);

                    (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)2);
                    tran.Commit();
                    (await repo.Count(p => p.Id, transaction: tran)).ShouldBe((ulong)2);
                }

                (await repo.Count(p => p.Id)).ShouldBe((ulong)2);
            }
        }

        private static async Task When_inserting_and_updating_with_transaction_commited()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                var person = new Person { Name = "P1", Age = 10 };
                await repo.Insert(person);

                var insertedPerson = (await repo.Get()).Single();

                var updatedPerson = new Person { Id = insertedPerson.Id, Name = "P1-updated", Age = 15 };

                using (var tran = repo.Connection.BeginTransaction())
                {
                    await repo.Update(updatedPerson, transaction: tran);

                    var snapshot1 = (await repo.Get(tran)).Single();
                    snapshot1.Id.ShouldBe(insertedPerson.Id);
                    snapshot1.Name.ShouldBe(updatedPerson.Name);
                    snapshot1.Age.ShouldBe(updatedPerson.Age);

                    tran.Commit();

                    var snapshot2 = (await repo.Get(tran)).Single();
                    snapshot2.Id.ShouldBe(insertedPerson.Id);
                    snapshot2.Name.ShouldBe(updatedPerson.Name);
                    snapshot2.Age.ShouldBe(updatedPerson.Age);
                }

                var snapshot3 = (await repo.Get()).Single();
                snapshot3.Id.ShouldBe(insertedPerson.Id);
                snapshot3.Name.ShouldBe(updatedPerson.Name);
                snapshot3.Age.ShouldBe(updatedPerson.Age);
            }
        }

        private static async Task When_inserting_and_updating_with_transaction_rolled_back()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                var person = new Person { Name = "P1", Age = 10 };
                await repo.Insert(person);

                var insertedPerson = (await repo.Get()).Single();

                var updatedPerson = new Person { Id = insertedPerson.Id, Name = "P1-updated", Age = 15 };

                using (var tran = repo.Connection.BeginTransaction())
                {
                    await repo.Update(updatedPerson, transaction: tran);

                    var snapshot1 = (await repo.Get(tran)).Single();
                    snapshot1.Id.ShouldBe(insertedPerson.Id);
                    snapshot1.Name.ShouldBe(updatedPerson.Name);
                    snapshot1.Age.ShouldBe(updatedPerson.Age);

                    tran.Rollback();

                    var snapshot2 = (await repo.Get(tran)).Single();
                    snapshot2.Id.ShouldBe(insertedPerson.Id);
                    snapshot2.Name.ShouldBe(insertedPerson.Name);
                    snapshot2.Age.ShouldBe(insertedPerson.Age);
                }

                var snapshot3 = (await repo.Get()).Single();
                snapshot3.Id.ShouldBe(insertedPerson.Id);
                snapshot3.Name.ShouldBe(insertedPerson.Name);
                snapshot3.Age.ShouldBe(insertedPerson.Age);
            }
        }

        private static async Task When_inserting_and_updating_with_transaction_with_isolation_level_commited()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                var person = new Person { Name = "P1", Age = 10 };
                await repo.Insert(person);

                var insertedPerson = (await repo.Get()).Single();

                var updatedPerson = new Person { Id = insertedPerson.Id, Name = "P1-updated", Age = 15 };

                using (var tran = repo.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    await repo.Update(updatedPerson, transaction: tran);

                    var snapshot1 = (await repo.Get(tran)).Single();
                    snapshot1.Id.ShouldBe(insertedPerson.Id);
                    snapshot1.Name.ShouldBe(updatedPerson.Name);
                    snapshot1.Age.ShouldBe(updatedPerson.Age);

                    tran.Commit();

                    var snapshot2 = (await repo.Get(tran)).Single();
                    snapshot2.Id.ShouldBe(insertedPerson.Id);
                    snapshot2.Name.ShouldBe(updatedPerson.Name);
                    snapshot2.Age.ShouldBe(updatedPerson.Age);
                }

                var snapshot3 = (await repo.Get()).Single();
                snapshot3.Id.ShouldBe(insertedPerson.Id);
                snapshot3.Name.ShouldBe(updatedPerson.Name);
                snapshot3.Age.ShouldBe(updatedPerson.Age);
            }
        }

        private static async Task When_inserting_and_updating_with_transaction_with_isolation_level_rolled_back()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                await conn.ExecuteAsync(DefaultTableQuery);

                var repo = conn.GetDBContext<Person>(SQLServerDialect.Instance);
                await repo.DeleteAll();
                await conn.ExecuteAsync("DBCC CHECKIDENT (Person, RESEED, 0)");

                var person = new Person { Name = "P1", Age = 10 };
                await repo.Insert(person);

                var insertedPerson = (await repo.Get()).Single();

                var updatedPerson = new Person { Id = insertedPerson.Id, Name = "P1-updated", Age = 15 };

                using (var tran = repo.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    await repo.Update(updatedPerson, transaction: tran);

                    var snapshot1 = (await repo.Get(tran)).Single();
                    snapshot1.Id.ShouldBe(insertedPerson.Id);
                    snapshot1.Name.ShouldBe(updatedPerson.Name);
                    snapshot1.Age.ShouldBe(updatedPerson.Age);

                    tran.Rollback();

                    var snapshot2 = (await repo.Get(tran)).Single();
                    snapshot2.Id.ShouldBe(insertedPerson.Id);
                    snapshot2.Name.ShouldBe(insertedPerson.Name);
                    snapshot2.Age.ShouldBe(insertedPerson.Age);
                }

                var snapshot3 = (await repo.Get()).Single();
                snapshot3.Id.ShouldBe(insertedPerson.Id);
                snapshot3.Name.ShouldBe(insertedPerson.Name);
                snapshot3.Age.ShouldBe(insertedPerson.Age);
            }
        }
    }
}