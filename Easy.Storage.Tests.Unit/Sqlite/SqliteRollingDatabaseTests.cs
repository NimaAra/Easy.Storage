namespace Easy.Storage.Tests.Unit.Sqlite
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.Sqlite;
    using Easy.Storage.Sqlite.Models;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class SqliteRollingDatabaseTests : Context
    {
        [Test]
        public async Task When_rolling_a_sqlite_database()
        {
            var dbFile = Path.GetTempFileName();
            var fileInfo = new FileInfo(dbFile);
            var originalConnectionString = SqliteConnectionBuilder.GetFileConnectionString(fileInfo);
            var dbDir = fileInfo.DirectoryName;
            var dbFileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileInfo.FullName);
            fileInfo.Delete();

            Console.WriteLine("DB File: " + dbFile);

            using (var db = new SqliteDatabase(new SqliteConnectionWrapper(originalConnectionString, TimeSpan.FromSeconds(3))))
            {
                await db.ExecuteAsync(TableQuery);
                await db.ExecuteAsync(ViewQuery);
                await db.ExecuteAsync(TriggerQuery);
                await db.ExecuteAsync(IndexQuery);

                var snapshot1 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot1.ShouldNotBeNull();
                snapshot1.Length.ShouldBe(4);

                snapshot1[0].Type.ShouldBe(SqliteObjectType.Table);
                snapshot1[0].Name.ShouldBe("Person");
                snapshot1[0].Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot1[1].Type.ShouldBe(SqliteObjectType.View);
                snapshot1[1].Name.ShouldBe("Person_view");
                snapshot1[1].Sql.ShouldBe(ViewQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot1[2].Type.ShouldBe(SqliteObjectType.Trigger);
                snapshot1[2].Name.ShouldBe("Person_bu");
                snapshot1[2].Sql.ShouldBe(TriggerQuery.Replace("IF NOT EXISTS ", string.Empty));

                snapshot1[3].Type.ShouldBe(SqliteObjectType.Index);
                snapshot1[3].Name.ShouldBe("Person_idx");
                snapshot1[3].Sql.ShouldBe(IndexQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                await Task.Delay(TimeSpan.FromSeconds(3));
                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)1);

                var repo = db.GetRepository<Person>();
                var p1 = new Person { Name = "P1", Age = 10 };
                await repo.InsertAsync(p1);

                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)2);

                var rolledFilesFirstRoll = Directory.GetFiles(dbDir, dbFileNameWithoutExtension + "_[*][*].tmp");
                rolledFilesFirstRoll.ShouldNotBeNull();
                rolledFilesFirstRoll.ShouldNotBeEmpty();
                rolledFilesFirstRoll.Length.ShouldBe(2);

                await Task.Delay(TimeSpan.FromSeconds(3));
                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)2);

                var p2 = new Person { Name = "P2", Age = 20 };
                await repo.InsertAsync(p2);

                (await db.ExistsAsync<Person>()).ShouldBeTrue();
                ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)3);

                var rolledFilesSecondRoll = Directory.GetFiles(dbDir, dbFileNameWithoutExtension + "_[*][*].tmp");
                rolledFilesSecondRoll.ShouldNotBeNull();
                rolledFilesSecondRoll.ShouldNotBeEmpty();
                rolledFilesSecondRoll.Length.ShouldBe(3);

                var snapshot2 = (await db.GetDatabaseObjectsAsync()).ToArray();
                snapshot2.ShouldNotBeNull();
                snapshot2.Length.ShouldBe(4);

                snapshot2[0].Type.ShouldBe(SqliteObjectType.Table);
                snapshot2[0].Name.ShouldBe("Person");
                snapshot2[0].Sql.ShouldBe(TableQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot2[1].Type.ShouldBe(SqliteObjectType.View);
                snapshot2[1].Name.ShouldBe("Person_view");
                snapshot2[1].Sql.ShouldBe(ViewQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                snapshot2[2].Type.ShouldBe(SqliteObjectType.Trigger);
                snapshot2[2].Name.ShouldBe("Person_bu");
                snapshot2[2].Sql.ShouldBe(TriggerQuery.Replace("IF NOT EXISTS ", string.Empty));

                snapshot2[3].Type.ShouldBe(SqliteObjectType.Index);
                snapshot2[3].Name.ShouldBe("Person_idx");
                snapshot2[3].Sql.ShouldBe(IndexQuery.Replace("IF NOT EXISTS ", string.Empty).Replace(";", string.Empty));

                var totalDbFiles = Directory.GetFiles(dbDir, dbFileNameWithoutExtension + "*.tmp");
                Array.ForEach(totalDbFiles, Console.WriteLine);
                totalDbFiles.Length.ShouldBe(3);

                Array.ForEach(rolledFilesSecondRoll, File.Delete);
            }
        }
    }
}