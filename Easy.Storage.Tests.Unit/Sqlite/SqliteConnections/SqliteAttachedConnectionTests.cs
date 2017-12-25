// ReSharper disable ObjectCreationAsStatement
namespace Easy.Storage.Tests.Unit.SQLite.SQLiteConnections
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.IO;
    using System.Reflection;
    using System.Threading.Tasks;
    using Easy.Storage.Common.Extensions;
    using Easy.Storage.SQLite.Connections;
    using Easy.Storage.SQLite.Extensions;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class SQLiteAttachedConnectionTests
    {
        [Test]
        public async Task When_creating_connection()
        {
            var sampleDbs = GetSampleDBs();

            using (var conn = new SQLiteAttachedConnection(sampleDbs))
            {
                conn.FilesToAttach.ShouldBe(sampleDbs);

                conn.ConnectionString.ShouldBe("Data Source=:memory:");
                conn.ConnectionTimeout.ShouldBe(15);
                conn.DataSource.ShouldBeNull();
                conn.Database.ShouldBe("main");
                conn.ServerVersion.ShouldBe("3.21.0");
                conn.State.ShouldBe(ConnectionState.Closed);

                var attachedDbs = await conn.GetAttachedDatabases();
                attachedDbs.Count.ShouldBe(4);

                attachedDbs["main"].ShouldBeNull();
                attachedDbs["a"].FullName.ShouldBe(sampleDbs["a"].FullName);
                attachedDbs["b"].FullName.ShouldBe(sampleDbs["b"].FullName);
                attachedDbs["c"].FullName.ShouldBe(sampleDbs["c"].FullName);

                conn.Close();

                attachedDbs = await conn.GetAttachedDatabases();
                attachedDbs.Count.ShouldBe(4);

                conn.DataSource.ShouldBe("");
            }
        }

        [Test]
        public void When_creating_connection_with_null_files()
        {
            Should.Throw<ArgumentException>(() => new SQLiteAttachedConnection(null))
                .Message.ShouldBe("Value cannot be null.\r\nParameter name: dbFiles");
        }

        [Test]
        public void When_creating_connection_with_empty_files()
        {
            Should.Throw<ArgumentException>(() => new SQLiteAttachedConnection(new Dictionary<string, FileInfo>()))
                .Message.ShouldBe("The dbFiles cannot be empty.");
        }

        [Test]
        public void When_changing_connection_state()
        {
            var sampleDbs = GetSampleDBs();

            var conn = new SQLiteAttachedConnection(sampleDbs);
            conn.State.ShouldBe(ConnectionState.Closed);

            conn.Open();

            conn.State.ShouldBe(ConnectionState.Open);

            conn.Close();
            conn.State.ShouldBe(ConnectionState.Closed);

            conn.Dispose();

            Should.Throw<ObjectDisposedException>(() => conn.State.ShouldBe(ConnectionState.Closed))
                .Message.ShouldBe("Cannot access a disposed object.\r\nObject name: 'SQLiteConnection'.");
        }

        [Test]
        public async Task When_querying_attached_files()
        {
            var sampleDbs = GetSampleDBs();

            using (var conn = new SQLiteAttachedConnection(sampleDbs))
            {
                conn.FilesToAttach.ShouldBe(sampleDbs);
                conn.State.ShouldBe(ConnectionState.Closed);
                
                var attachedDbs = await conn.GetAttachedDatabases();

                attachedDbs.Count.ShouldBe(4);

                attachedDbs.ShouldContainKey("main");
                attachedDbs["main"].ShouldBeNull();

                attachedDbs.ShouldContainKey("a");
                attachedDbs["a"].FullName.ShouldBe(sampleDbs["a"].FullName);

                attachedDbs.ShouldContainKey("b");
                attachedDbs["b"].FullName.ShouldBe(sampleDbs["b"].FullName);

                attachedDbs.ShouldContainKey("c");
                attachedDbs["c"].FullName.ShouldBe(sampleDbs["c"].FullName);

                (await conn.ExecuteScalarAsync<int>("SELECT COUNT(Id) FROM a.Person"))
                    .ShouldBe(3);

                (await conn.ExecuteScalarAsync<int>("SELECT COUNT(Id) FROM b.Person"))
                    .ShouldBe(3);

                (await conn.ExecuteScalarAsync<int>("SELECT COUNT(Id) FROM c.Person"))
                    .ShouldBe(3);

                (await conn.ExecuteAsync("DELETE FROM b.Person"))
                    .ShouldBe(3);

                (await conn.ExecuteScalarAsync<int>("SELECT COUNT(Id) FROM b.Person"))
                    .ShouldBe(0);
            }
        }

        private static Dictionary<string, FileInfo> GetSampleDBs()
        {
            var dbsDirectory = Path.Combine(Path.GetDirectoryName(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath), "Sqlite\\SampleDbs");

            var sampleDbs = new Dictionary<string, FileInfo>
            {
                {"a", new FileInfo(Path.Combine(dbsDirectory, "1.db"))},
                {"b", new FileInfo(Path.Combine(dbsDirectory, "2.db"))},
                {"c", new FileInfo(Path.Combine(dbsDirectory, "3.db"))}
            };
            return sampleDbs;
        }
    }
}