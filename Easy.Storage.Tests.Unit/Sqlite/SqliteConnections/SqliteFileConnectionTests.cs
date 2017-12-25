// ReSharper disable ObjectCreationAsStatement

namespace Easy.Storage.Tests.Unit.SQLite.SQLiteConnections
{
    using System;
    using System.Data;
    using System.IO;
    using Easy.Storage.SQLite;
    using Easy.Storage.SQLite.Connections;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    // ReSharper disable once InconsistentNaming
    internal sealed class SQLiteFileConnectionTests
    {
        [Test]
        public void When_creating_connection()
        {
            using (var conn = new SQLiteFileConnection(new FileInfo("C:\\SomeFile.db")))
            {
                conn.ConnectionString
                    .ShouldBe(@"data source=C:\SomeFile.db;failifmissing=False;pooling=False;binaryguid=False;datetimekind=Utc;datetimeformat=UnixEpoch;journal mode=Wal;synchronous=Off;useutf16encoding=False;read only=False;legacy format=False;page size=4096;cache size=-2000");
                conn.ConnectionTimeout.ShouldBe(15);
                conn.DataSource.ShouldBeNull();
                conn.Database.ShouldBe("main");
                conn.ServerVersion.ShouldBe("3.21.0");
                conn.State.ShouldBe(ConnectionState.Closed);
            }
        }

        [Test]
        public void When_creating_connection_with_valid_connection_string()
        {
            using (var conn = new SQLiteFileConnection(SQLiteConnectionStringProvider.GetFileConnectionString(new FileInfo("C:\\SomeFile.db"))))
            {
                conn.ConnectionString
                    .ShouldBe(@"data source=C:\SomeFile.db;failifmissing=False;pooling=False;binaryguid=False;datetimekind=Utc;datetimeformat=UnixEpoch;journal mode=Wal;synchronous=Off;useutf16encoding=False;read only=False;legacy format=False;page size=4096;cache size=-2000");
                conn.ConnectionTimeout.ShouldBe(15);
                conn.DataSource.ShouldBeNull();
                conn.Database.ShouldBe("main");
                conn.ServerVersion.ShouldBe("3.21.0");
                conn.State.ShouldBe(ConnectionState.Closed);
            }
        }

        [Test]
        public void When_creating_connection_with_null_empty_and_whitespace_connection_string()
        {
            Should.Throw<ArgumentException>(() => new SQLiteFileConnection((string) null))
                .Message.ShouldBe("Connection string cannot be null, empty or whitespace.");

            Should.Throw<ArgumentException>(() => new SQLiteFileConnection((FileInfo) null))
                .Message.ShouldBe("Value cannot be null.\r\nParameter name: file");

            Should.Throw<ArgumentException>(() => new SQLiteFileConnection(string.Empty))
                .Message.ShouldBe("Connection string cannot be null, empty or whitespace.");

            Should.Throw<ArgumentException>(() => new SQLiteFileConnection(" "))
                .Message.ShouldBe("Connection string cannot be null, empty or whitespace.");
        }

        [Test]
        public void When_creating_connection_with_non_memory_connection_string()
        {
            Should.Throw<ArgumentException>(() => new SQLiteFileConnection(":memory:"))
                .Message.ShouldBe("Cannot be a SQLite memory connection-string.");
        }

        [Test]
        public void When_changing_connection_state()
        {
            var fileInfo = new FileInfo(Path.GetTempFileName());
            try
            {
                var conn = new SQLiteFileConnection(fileInfo);
                conn.State.ShouldBe(ConnectionState.Closed);

                conn.Open();
                conn.State.ShouldBe(ConnectionState.Open);

                conn.Close();
                conn.State.ShouldBe(ConnectionState.Open);

                conn.Dispose();

                Should.Throw<ObjectDisposedException>(() => conn.State.ShouldBe(ConnectionState.Closed))
                    .Message.ShouldBe("Cannot access a disposed object.\r\nObject name: 'SQLiteConnection'.");
            } catch (Exception)
            {
                fileInfo.Delete();
            }
        }
    }
}