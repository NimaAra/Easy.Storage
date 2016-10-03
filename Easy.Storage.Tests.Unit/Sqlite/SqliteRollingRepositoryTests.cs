namespace Easy.Storage.Tests.Unit.Sqlite
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Easy.Storage.Common;
    using Easy.Storage.Sqlite;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;
    using Easy.Storage.Common.Extensions;

    [TestFixture]
    internal sealed class SqliteRollingRepositoryTests : Context
    {
        [Test]
        public async Task When_counting_non_aliased_model()
        {
            var dbFile = Path.GetTempFileName();
            var fileInfo = new FileInfo(dbFile);
            var dbFileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileInfo.FullName);

            try
            {
                IDatabase db = new SqliteDatabase(new SqliteConnectionWrapper(SqliteConnectionBuilder.GetFileConnectionString(fileInfo), TimeSpan.FromSeconds(2)));
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.CountAsync(p => p.Id)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Name)).ShouldBe((ulong)0);

                (await repo.CountAsync(p => p.Id, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Name, true)).ShouldBe((ulong)0);

                var people = new[]
                {
                    new Person {Name = "P1", Age = 10},
                    new Person {Name = "P2", Age = 30},
                    new Person {Id = 123, Name = "P3", Age = 30},
                    new Person {Name = "P3", Age = 30}
                };

                (await repo.InsertAsync(people)).ShouldBe(people.Length);

                await Task.Delay(TimeSpan.FromSeconds(3));

                (await repo.CountAsync(p => p.Id)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.Name)).ShouldBe((ulong)people.Length);

                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)1);

                (await db.ExistsAsync<Person>()).ShouldBeTrue();

                var rolledFiles = Directory.GetFiles(fileInfo.DirectoryName, dbFileNameWithoutExtension + "_[*][*].tmp");
                rolledFiles.ShouldNotBeNull();
                rolledFiles.ShouldNotBeEmpty();
                rolledFiles.Length.ShouldBe(1);

                (await repo.CountAsync(p => p.Id)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Name)).ShouldBe((ulong)0);

                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)1);

                (await db.ExistsAsync<Person>()).ShouldBeTrue();

                var rolledFilesFirstRoll = Directory.GetFiles(fileInfo.DirectoryName,
                    dbFileNameWithoutExtension + "_[*][*].tmp");
                rolledFilesFirstRoll.ShouldNotBeNull();
                rolledFilesFirstRoll.ShouldNotBeEmpty();
                rolledFilesFirstRoll.Length.ShouldBe(1);

                db.Dispose();
                Array.ForEach(rolledFiles, File.Delete);
            }
            finally
            {
                fileInfo.Delete();
            }
        }

        [Test]
        public async Task When_counting_aliased_model()
        {
            var dbFile = Path.GetTempFileName();
            var fileInfo = new FileInfo(dbFile);
            var dbFileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileInfo.FullName);

            try
            {
                IDatabase db = new SqliteDatabase(new SqliteConnectionWrapper(SqliteConnectionBuilder.GetFileConnectionString(fileInfo), TimeSpan.FromSeconds(2)));
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.CountAsync(p => p.SomeId)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.SomeName)).ShouldBe((ulong)0);

                (await repo.CountAsync(p => p.SomeId, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.SomeName, true)).ShouldBe((ulong)0);

                var people = new[]
                {
                    new MyPerson {SomeName = "P1", Age = 10},
                    new MyPerson {SomeName = "P2", Age = 30},
                    new MyPerson {SomeId = 123, SomeName = "P3", Age = 30},
                    new MyPerson {SomeName = "P3", Age = 30}
                };

                (await repo.InsertAsync(people)).ShouldBe(people.Length);

                await Task.Delay(TimeSpan.FromSeconds(3));

                (await repo.CountAsync(p => p.SomeId)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.SomeName)).ShouldBe((ulong)people.Length);

                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)1);

                (await db.ExistsAsync<MyPerson>()).ShouldBeTrue();

                var rolledFiles = Directory.GetFiles(fileInfo.DirectoryName, dbFileNameWithoutExtension + "_[*][*].tmp");
                rolledFiles.ShouldNotBeNull();
                rolledFiles.ShouldNotBeEmpty();
                rolledFiles.Length.ShouldBe(1);

                (await repo.CountAsync(p => p.SomeId)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.SomeName)).ShouldBe((ulong)0);

                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)1);

                (await db.ExistsAsync<MyPerson>()).ShouldBeTrue();

                var rolledFilesFirstRoll = Directory.GetFiles(fileInfo.DirectoryName,
                    dbFileNameWithoutExtension + "_[*][*].tmp");
                rolledFilesFirstRoll.ShouldNotBeNull();
                rolledFilesFirstRoll.ShouldNotBeEmpty();
                rolledFilesFirstRoll.Length.ShouldBe(1);

                db.Dispose();
                Array.ForEach(rolledFiles, File.Delete);
            }
            finally
            {
                fileInfo.Delete();
            }
        }
    }
}