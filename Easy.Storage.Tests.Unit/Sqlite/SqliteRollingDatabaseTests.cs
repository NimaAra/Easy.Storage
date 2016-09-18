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
            var dbFileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileInfo.FullName);

            try
            {
                using (var db = new SqliteDatabase(SqliteConnectionBuilder.GetFileConnectionString(fileInfo), TimeSpan.FromSeconds(3)))
                {
                    await db.Connection.ExecuteAsync(TableQuery);
                    await db.Connection.ExecuteAsync(ViewQuery);
                    await db.Connection.ExecuteAsync(TriggerQuery);
                    await db.Connection.ExecuteAsync(IndexQuery);

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

                    (await db.ExistsAsync<Person>()).ShouldBeTrue();

                    var rolledFilesFirstRoll = Directory.GetFiles(fileInfo.DirectoryName, dbFileNameWithoutExtension + "_[*][*].tmp");
                    rolledFilesFirstRoll.ShouldNotBeNull();
                    rolledFilesFirstRoll.ShouldNotBeEmpty();
                    rolledFilesFirstRoll.Length.ShouldBe(1);

                    await Task.Delay(TimeSpan.FromSeconds(3));
                    ((SqliteConnectionWrapper)db.Connection).RollCount.ShouldBe((uint)2);

                    (await db.ExistsAsync<Person>()).ShouldBeTrue();

                    var rolledFilesSecondRoll = Directory.GetFiles(fileInfo.DirectoryName, dbFileNameWithoutExtension + "_[*][*].tmp");
                    rolledFilesSecondRoll.ShouldNotBeNull();
                    rolledFilesSecondRoll.ShouldNotBeEmpty();
                    rolledFilesSecondRoll.Length.ShouldBe(2);

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

                    Array.ForEach(rolledFilesSecondRoll, File.Delete);
                }
            } finally
            {
              fileInfo.Delete();  
            }
        }
    }
}