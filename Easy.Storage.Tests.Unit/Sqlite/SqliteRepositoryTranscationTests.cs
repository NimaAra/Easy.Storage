namespace Easy.Storage.Tests.Unit.Sqlite
{
    using System.Data;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.Sqlite.Connections;
    using Easy.Storage.Sqlite.Extensions;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class SqliteRepositoryTranscationTests : Context
    {
        [Test]
        public async Task When_querying_with_transaction()
        {
            var fileInfo = new FileInfo(Path.GetTempFileName());

            try
            {
                using (var conn = new SqliteFileConnection(fileInfo))
                {
                    conn.Open();

                    await conn.ExecuteAsync(TableQuery);

                    var person = new Person {Name = "P1", Age = 10};

                    var repo = conn.GetRepository<Person>();
                    await repo.InsertAsync(person);

                    using (var tran = conn.BeginTransaction())
                    {
                        (await repo.CountAsync(p => p.Id, transaction: tran)).ShouldBe((ulong) 1);
                    }

                    (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 1);
                }
            } finally
            {
                fileInfo.Delete();
            }
        }

        [Test]
        public async Task When_inserting_with_transaction_commited()
        {
            var fileInfo = new FileInfo(Path.GetTempFileName());

            try
            {
                using (var conn = new SqliteFileConnection(fileInfo))
                {
                    conn.Open();

                    await conn.ExecuteAsync(TableQuery);

                    var person = new Person {Name = "P1", Age = 10};

                    var repo = conn.GetRepository<Person>();

                    using (var tran = conn.BeginTransaction())
                    {
                        await repo.InsertAsync(person);
                        (await repo.CountAsync(p => p.Id, transaction: tran)).ShouldBe((ulong) 1);
                        tran.Commit();

                        (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 1);
                    }

                    (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 1);
                }
            } finally
            {
                fileInfo.Delete();
            }
        }

        [Test]
        public async Task When_inserting_with_transaction_rolled_back()
        {
            var fileInfo = new FileInfo(Path.GetTempFileName());

            try
            {
                using (var conn = new SqliteFileConnection(fileInfo))
                {
                    conn.Open();
                    await conn.ExecuteAsync(TableQuery);

                    var person = new Person {Name = "P1", Age = 10};

                    var repo = conn.GetRepository<Person>();
                    using (var tran = conn.BeginTransaction())
                    {
                        await repo.InsertAsync(person);
                        (await repo.CountAsync(p => p.Id, transaction: tran)).ShouldBe((ulong) 1);

                        (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 1);
                        tran.Rollback();
                        (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 0);
                    }

                    (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 0);
                }
            } finally
            {
                fileInfo.Delete();
            }
        }

        [Test]
        public async Task When_inserting_with_transaction_disposed()
        {
            var fileInfo = new FileInfo(Path.GetTempFileName());

            try
            {
                using (var conn = new SqliteFileConnection(fileInfo))
                {
                    conn.Open();
                    await conn.ExecuteAsync(TableQuery);

                    var person = new Person {Name = "P1", Age = 10};

                    var repo = conn.GetRepository<Person>();
                    using (var tran = conn.BeginTransaction())
                    {
                        await repo.InsertAsync(person);
                        (await repo.CountAsync(p => p.Id, transaction: tran)).ShouldBe((ulong) 1);

                        (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 1);
                    }

                    (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 0);
                }
            } finally
            {
                fileInfo.Delete();
            }
        }

        [Test]
        public async Task When_inserting_multiple_with_transaction_disposed_one_inside_the_other_outside()
        {
            var fileInfo = new FileInfo(Path.GetTempFileName());

            try
            {
                using (var conn = new SqliteFileConnection(fileInfo))
                {
                    conn.Open();
                    await conn.ExecuteAsync(TableQuery);

                    var person1 = new Person {Name = "P1", Age = 10};

                    var repo = conn.GetRepository<Person>();
                    await repo.InsertAsync(person1);

                    using (var tran = conn.BeginTransaction())
                    {
                        var person2 = new Person {Name = "P2", Age = 20};
                        await repo.InsertAsync(person2);
                        (await repo.CountAsync(p => p.Id, transaction: tran)).ShouldBe((ulong) 2);

                        (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 2);
                    }

                    (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 1);
                }
            } finally
            {
                fileInfo.Delete();
            }
        }

        [Test]
        public async Task When_inserting_multiple_with_transaction_commited_one_inside_the_other_outside()
        {
            var fileInfo = new FileInfo(Path.GetTempFileName());

            try
            {
                using (var conn = new SqliteFileConnection(fileInfo))
                {
                    conn.Open();
                    await conn.ExecuteAsync(TableQuery);

                    var person1 = new Person {Name = "P1", Age = 10};

                    var repo = conn.GetRepository<Person>();
                    await repo.InsertAsync(person1);

                    using (var tran = conn.BeginTransaction())
                    {
                        var person2 = new Person {Name = "P2", Age = 20};
                        await repo.InsertAsync(person2);
                        (await repo.CountAsync(p => p.Id, transaction: tran)).ShouldBe((ulong) 2);

                        (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 2);
                        tran.Commit();
                        (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 2);
                    }

                    (await repo.CountAsync(p => p.Id)).ShouldBe((ulong) 2);
                }
            } finally
            {
                fileInfo.Delete();
            }
        }

        [Test]
        public async Task When_inserting_and_updating_with_transaction_commited()
        {
            var fileInfo = new FileInfo(Path.GetTempFileName());

            try
            {
                using (var conn = new SqliteFileConnection(fileInfo))
                {
                    conn.Open();
                    await conn.ExecuteAsync(TableQuery);

                    var person = new Person {Name = "P1", Age = 10};

                    var repo = conn.GetRepository<Person>();
                    await repo.InsertAsync(person);

                    var insertedPerson = (await repo.GetAsync()).Single();

                    var updatedPerson = new Person {Id = insertedPerson.Id, Name = "P1-updated", Age = 15};

                    using (var tran = conn.BeginTransaction())
                    {
                        await repo.UpdateAsync(updatedPerson);

                        var snapshot1 = (await repo.GetAsync()).Single();
                        snapshot1.Id.ShouldBe(insertedPerson.Id);
                        snapshot1.Name.ShouldBe(updatedPerson.Name);
                        snapshot1.Age.ShouldBe(updatedPerson.Age);

                        tran.Commit();

                        var snapshot2 = (await repo.GetAsync()).Single();
                        snapshot2.Id.ShouldBe(insertedPerson.Id);
                        snapshot2.Name.ShouldBe(updatedPerson.Name);
                        snapshot2.Age.ShouldBe(updatedPerson.Age);
                    }

                    var snapshot3 = (await repo.GetAsync()).Single();
                    snapshot3.Id.ShouldBe(insertedPerson.Id);
                    snapshot3.Name.ShouldBe(updatedPerson.Name);
                    snapshot3.Age.ShouldBe(updatedPerson.Age);
                }
            } finally
            {
                fileInfo.Delete();
            }
        }

        [Test]
        public async Task When_inserting_and_updating_with_transaction_rolled_back()
        {
            var fileInfo = new FileInfo(Path.GetTempFileName());

            try
            {
                using (var conn = new SqliteFileConnection(fileInfo))
                {
                    conn.Open();
                    await conn.ExecuteAsync(TableQuery);

                    var person = new Person {Name = "P1", Age = 10};

                    var repo = conn.GetRepository<Person>();
                    await repo.InsertAsync(person);

                    var insertedPerson = (await repo.GetAsync()).Single();

                    var updatedPerson = new Person {Id = insertedPerson.Id, Name = "P1-updated", Age = 15};

                    using (var tran = conn.BeginTransaction())
                    {
                        await repo.UpdateAsync(updatedPerson);

                        var snapshot1 = (await repo.GetAsync()).Single();
                        snapshot1.Id.ShouldBe(insertedPerson.Id);
                        snapshot1.Name.ShouldBe(updatedPerson.Name);
                        snapshot1.Age.ShouldBe(updatedPerson.Age);

                        tran.Rollback();

                        var snapshot2 = (await repo.GetAsync()).Single();
                        snapshot2.Id.ShouldBe(insertedPerson.Id);
                        snapshot2.Name.ShouldBe(insertedPerson.Name);
                        snapshot2.Age.ShouldBe(insertedPerson.Age);
                    }

                    var snapshot3 = (await repo.GetAsync()).Single();
                    snapshot3.Id.ShouldBe(insertedPerson.Id);
                    snapshot3.Name.ShouldBe(insertedPerson.Name);
                    snapshot3.Age.ShouldBe(insertedPerson.Age);
                }
            } finally
            {
                fileInfo.Delete();
            }
        }

        [Test]
        public async Task When_inserting_and_updating_with_transaction_with_isolation_level_commited()
        {
            var fileInfo = new FileInfo(Path.GetTempFileName());

            try
            {
                using (var conn = new SqliteFileConnection(fileInfo))
                {
                    conn.Open();
                    await conn.ExecuteAsync(TableQuery);

                    var person = new Person {Name = "P1", Age = 10};

                    var repo = conn.GetRepository<Person>();
                    await repo.InsertAsync(person);

                    var insertedPerson = (await repo.GetAsync()).Single();

                    var updatedPerson = new Person {Id = insertedPerson.Id, Name = "P1-updated", Age = 15};

                    using (var tran = conn.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        await repo.UpdateAsync(updatedPerson);

                        var snapshot1 = (await repo.GetAsync()).Single();
                        snapshot1.Id.ShouldBe(insertedPerson.Id);
                        snapshot1.Name.ShouldBe(updatedPerson.Name);
                        snapshot1.Age.ShouldBe(updatedPerson.Age);

                        tran.Commit();

                        var snapshot2 = (await repo.GetAsync()).Single();
                        snapshot2.Id.ShouldBe(insertedPerson.Id);
                        snapshot2.Name.ShouldBe(updatedPerson.Name);
                        snapshot2.Age.ShouldBe(updatedPerson.Age);
                    }

                    var snapshot3 = (await repo.GetAsync()).Single();
                    snapshot3.Id.ShouldBe(insertedPerson.Id);
                    snapshot3.Name.ShouldBe(updatedPerson.Name);
                    snapshot3.Age.ShouldBe(updatedPerson.Age);
                }
            } finally
            {
                fileInfo.Delete();
            }

        }

        [Test]
        public async Task When_inserting_and_updating_with_transaction_with_isolation_level_rolled_back()
        {
            var fileInfo = new FileInfo(Path.GetTempFileName());

            try
            {
                using (var conn = new SqliteFileConnection(fileInfo))
                {
                    conn.Open();
                    await conn.ExecuteAsync(TableQuery);

                    var person = new Person { Name = "P1", Age = 10 };

                    var repo = conn.GetRepository<Person>();
                    await repo.InsertAsync(person);

                    var insertedPerson = (await repo.GetAsync()).Single();

                    var updatedPerson = new Person { Id = insertedPerson.Id, Name = "P1-updated", Age = 15 };

                    using (var tran = conn.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        await repo.UpdateAsync(updatedPerson);

                        var snapshot1 = (await repo.GetAsync()).Single();
                        snapshot1.Id.ShouldBe(insertedPerson.Id);
                        snapshot1.Name.ShouldBe(updatedPerson.Name);
                        snapshot1.Age.ShouldBe(updatedPerson.Age);

                        tran.Rollback();

                        var snapshot2 = (await repo.GetAsync()).Single();
                        snapshot2.Id.ShouldBe(insertedPerson.Id);
                        snapshot2.Name.ShouldBe(insertedPerson.Name);
                        snapshot2.Age.ShouldBe(insertedPerson.Age);
                    }

                    var snapshot3 = (await repo.GetAsync()).Single();
                    snapshot3.Id.ShouldBe(insertedPerson.Id);
                    snapshot3.Name.ShouldBe(insertedPerson.Name);
                    snapshot3.Age.ShouldBe(insertedPerson.Age);
                }
            } finally
            {
                fileInfo.Delete();
            }
        }
    }
}