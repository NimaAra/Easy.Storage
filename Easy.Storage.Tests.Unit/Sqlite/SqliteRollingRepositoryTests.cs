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
        public async Task When_counting_aliased_model()
        {
            var dbFile = Path.GetTempFileName();
            var fileInfo = new FileInfo(dbFile);
            var originalConnectionString = SqliteConnectionBuilder.GetFileConnectionString(fileInfo);
            var dbDir = fileInfo.DirectoryName;
            var dbFileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileInfo.FullName);
            fileInfo.Delete();

            Console.WriteLine("DB File: " + dbFile);

            using (IDatabase db = new SqliteDatabase(new SqliteConnectionWrapper(originalConnectionString, TimeSpan.FromSeconds(2))))
            {
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

                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)1);

                (await repo.InsertAsync(people)).ShouldBe(4);

                await Task.Delay(TimeSpan.FromSeconds(3));

                (await repo.CountAsync(p => p.SomeId)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.SomeName)).ShouldBe((ulong)people.Length);

                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)1);

                var p5 = new MyPerson { SomeName = "P5", Age = 50 };
                await repo.InsertAsync(p5);
                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)2);

                (await db.ExistsAsync<MyPerson>()).ShouldBeTrue();

                var rolledFiles = Directory.GetFiles(dbDir, dbFileNameWithoutExtension + "_[*][*].tmp");
                rolledFiles.ShouldNotBeNull();
                rolledFiles.ShouldNotBeEmpty();
                rolledFiles.Length.ShouldBe(2);

                (await repo.CountAsync(p => p.SomeId)).ShouldBe((ulong)1);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)1);
                (await repo.CountAsync(p => p.SomeName)).ShouldBe((ulong)1);

                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)2);

                (await db.ExistsAsync<MyPerson>()).ShouldBeTrue();

                var rolledFilesFirstRoll = Directory.GetFiles(dbDir, dbFileNameWithoutExtension + "_[*][*].tmp");
                rolledFilesFirstRoll.ShouldNotBeNull();
                rolledFilesFirstRoll.ShouldNotBeEmpty();
                rolledFilesFirstRoll.Length.ShouldBe(2);

                var totalDbFiles = Directory.GetFiles(dbDir, dbFileNameWithoutExtension + "*.tmp");
                Array.ForEach(totalDbFiles, Console.WriteLine);
                totalDbFiles.Length.ShouldBe(2);

                Array.ForEach(rolledFiles, File.Delete);
            }
        }
    }
}